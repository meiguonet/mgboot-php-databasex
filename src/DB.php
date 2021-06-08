<?php

namespace mgboot\databasex;

use Illuminate\Support\Collection;
use mgboot\AppConf;
use mgboot\Cast;
use mgboot\constant\Regexp;
use mgboot\poolx\ConnectionInterface;
use mgboot\poolx\PdoConnection;
use mgboot\poolx\PoolContext;
use mgboot\swoole\Swoole;
use mgboot\util\ExceptionUtils;
use mgboot\util\FileUtils;
use mgboot\util\JsonUtils;
use mgboot\util\StringUtils;
use PDO;
use PDOStatement;
use Psr\Log\LoggerInterface;
use RuntimeException;
use Throwable;

final class DB
{
    private static ?LoggerInterface $logger;
    private static array $connectionSettings = [];
    private static bool $debugLogEnabled = false;
    private static string $tableSchemasCacheFile = 'classpath:cache/table_schemas.php';
    private static array $tableSchemas = [];

    private function __construct()
    {
    }

    private function __clone(): void
    {
    }

    public static function setLogger(LoggerInterface $logger): void
    {
        self::$logger = $logger;
    }

    public static function setConnectionSettings(array $settings): void{
        self::$connectionSettings = $settings;
    }

    public static function enableDebugLog(): void
    {
        self::$debugLogEnabled = true;
    }

    public static function buildTableSchemas(): array
    {
        $inDevMode = AppConf::getEnv() === 'dev';
        $inSwooleMode = Swoole::getWorkerId() > 0;
        $key = Swoole::buildGlobalVarKey();
        $schemas = [];

        if (!$inDevMode && !$inSwooleMode) {
            $schemas = self::getTableSchemasFromCacheFile();
        }

        if (is_array($schemas) && !empty($schemas)) {
            self::$tableSchemas[$key] = $schemas;
            return $schemas;
        }

        try {
            $conn = PdoConnection::create(self::$connectionSettings);
        } catch (Throwable) {
            return [];
        }

        $tables = [];

        try {
            $stmt = $conn->getRealConnection()->prepare('SHOW TABLES');
            $stmt->execute();
            $records = $stmt->fetchAll(PDO::FETCH_ASSOC);

            if (!is_array($records) || empty($records)) {
                $conn->close();
                return [];
            }

            foreach ($records as $record) {
                foreach ($record as $key => $value) {
                    if (str_contains($key, 'Tables_in')) {
                        $tables[] = trim($value);
                        break;
                    }
                }
            }
        } catch (Throwable) {
            $conn->close();
            return [];
        }

        if (empty($tables)) {
            $conn->close();
            return [];
        }

        $schemas = [];

        foreach ($tables as $tableName) {
            try {
                $stmt = $conn->getRealConnection()->prepare("DESC $tableName");
                $stmt->execute();
                $items = $stmt->fetchAll(PDO::FETCH_ASSOC);

                if (!is_array($items) || empty($items)) {
                    continue;
                }

                $schema = collect($items)->map(function ($item) {
                    $fieldName = $item['Field'];
                    $nullable = stripos($item['Null'], 'YES') !== false;
                    $isPrimaryKey = $item['Key'] === 'PRI';
                    $defaultValue = $item['Default'];
                    $autoIncrement = $item['Extra'] === 'auto_increment';
                    $parts = preg_split(Regexp::SPACE_SEP, $item['Type']);

                    if (str_contains($parts[0], '(')) {
                        $fieldType = StringUtils::substringBefore($parts[0], '(');
                        $fieldSize = str_replace($fieldType, '', $parts[0]);
                    } else {
                        $fieldType = $parts[0];
                        $fieldSize = '';
                    }

                    if (!str_starts_with($fieldSize, '(') || !str_ends_with($fieldSize, ')')) {
                        $fieldSize = '';
                    } else {
                        $fieldSize = rtrim(ltrim($fieldSize, '('), ')');
                    }

                    if (is_numeric($fieldSize)) {
                        $fieldSize = (int) $fieldSize;
                    }

                    $unsigned = stripos($item['Type'], 'unsigned') !== false;

                    return compact(
                        'fieldName',
                        'fieldType',
                        'fieldSize',
                        'unsigned',
                        'nullable',
                        'defaultValue',
                        'autoIncrement',
                        'isPrimaryKey'
                    );
                })->toArray();
            } catch (Throwable) {
                $schema = null;
            }

            if (!is_array($schema) || empty($schema)) {
                continue;
            }

            $schemas[$tableName] = $schema;
        }

        $conn->close();
        self::$tableSchemas[$key] = $schemas;

        if (!$inDevMode && !$inSwooleMode) {
            self::writeTableSchemasToCacheFile($schemas);
        }

        return $schemas;
    }

    public static function getTableSchema(string $tableName): array
    {
        $tableName = str_replace('`', '', $tableName);

        if (str_contains($tableName, '.')) {
            $tableName = StringUtils::substringAfterLast($tableName, '.');
        }

        $key = Swoole::buildGlobalVarKey();
        $schemas = self::$tableSchemas[$key];

        if (!is_array($schemas) || empty($schemas)) {
            $schemas = self::buildTableSchemas();
        }

        return is_array($schemas) && isset($schemas[$tableName]) ? $schemas[$tableName] : [];
    }

    public static function table(string $tableName): QueryBuilder
    {
        return QueryBuilder::create($tableName);
    }

    public static function raw(string $expr): Expression
    {
        return Expression::create($expr);
    }

    public static function selectBySql(string $sql, array $params = []): Collection
    {
        self::logSql($sql, $params);

        try {
            /* @var PdoConnection $conn */
            /* @var PDO $pdo */
            list($conn, $pdo) = self::getConnection();
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        }

        $hasError = false;

        try {
            $stmt = $pdo->prepare($sql);

            if (!($stmt instanceof PDOStatement)) {
                return collect([]);
            }

            self::pdoBindParams($stmt, $params);
            $stmt->execute();
            self::freeConnection($conn);
            return collect($stmt->fetchAll());
        } catch (Throwable $ex) {
            $hasError = true;
            $ex = self::wrapAsDbException($ex);
            self::freeConnection($conn, $ex);
            self::writeErrorLog($ex);
            throw $ex;
        } finally {
            if (!$hasError) {
                self::freeConnection($conn);
            }
        }
    }

    public static function firstBySql(string $sql, array $params = []): ?array
    {
        self::logSql($sql, $params);

        try {
            /* @var PdoConnection $conn */
            /* @var PDO $pdo */
            list($conn, $pdo) = self::getConnection();
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        }

        $hasError = false;

        try {
            $stmt = $pdo->prepare($sql);

            if (!($stmt instanceof PDOStatement)) {
                return null;
            }

            self::pdoBindParams($stmt, $params);
            $stmt->execute();
            $data = $stmt->fetch();
            return is_array($data) ? $data : null;
        } catch (Throwable $ex) {
            $hasError = true;
            $ex = self::wrapAsDbException($ex);
            self::freeConnection($conn, $ex);
            self::writeErrorLog($ex);
            throw $ex;
        } finally {
            if (!$hasError) {
                self::freeConnection($conn);
            }
        }
    }

    public static function countBySql(string $sql, array $params = []): int
    {
        self::logSql($sql, $params);

        try {
            /* @var PdoConnection $conn */
            /* @var PDO $pdo */
            list($conn, $pdo) = self::getConnection();
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        }

        $hasError = false;

        try {
            $stmt = $pdo->prepare($sql);

            if (!($stmt instanceof PDOStatement)) {
                return 0;
            }

            self::pdoBindParams($stmt, $params);
            $stmt->execute();
            return (int) $stmt->fetchColumn();
        } catch (Throwable $ex) {
            $hasError = true;
            $ex = self::wrapAsDbException($ex);
            self::freeConnection($conn, $ex);
            self::writeErrorLog($ex);
            throw $ex;
        } finally {
            if (!$hasError) {
                self::freeConnection($conn);
            }
        }
    }

    public static function insertBySql(string $sql, array $params = []): int
    {
        self::logSql($sql, $params);

        try {
            /* @var PdoConnection $conn */
            /* @var PDO $pdo */
            list($conn, $pdo) = self::getConnection();
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        }

        $hasError = false;

        try {
            $stmt = $pdo->prepare($sql);

            if (!($stmt instanceof PDOStatement)) {
                return 0;
            }

            self::pdoBindParams($stmt, $params);

            if (!$stmt->execute()) {
                return 0;
            }

            return (int) $pdo->lastInsertId();
        } catch (Throwable $ex) {
            $hasError = true;
            $ex = self::wrapAsDbException($ex);
            self::freeConnection($conn, $ex);
            self::writeErrorLog($ex);
            throw $ex;
        } finally {
            if (!$hasError) {
                self::freeConnection($conn);
            }
        }
    }

    public static function updateBySql(string $sql, array $params = []): int
    {
        self::logSql($sql, $params);

        try {
            /* @var PdoConnection $conn */
            /* @var PDO $pdo */
            list($conn, $pdo) = self::getConnection();
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        }

        $hasError = false;

        try {
            $stmt = $pdo->prepare($sql);

            if (!($stmt instanceof PDOStatement)) {
                return 0;
            }

            self::pdoBindParams($stmt, $params);

            if (!$stmt->execute()) {
                return 0;
            }

            return $stmt->rowCount();
        } catch (Throwable $ex) {
            $hasError = true;
            $ex = self::wrapAsDbException($ex);
            self::freeConnection($conn, $ex);
            self::writeErrorLog($ex);
            throw $ex;
        } finally {
            if (!$hasError) {
                self::freeConnection($conn);
            }
        }
    }

    public static function sumBySql(string $sql, array $params = []): int|float|string
    {
        self::logSql($sql, $params);

        try {
            /* @var PdoConnection $conn */
            /* @var PDO $pdo */
            list($conn, $pdo) = self::getConnection();
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        }

        $hasError = false;

        try {
            $stmt = $pdo->prepare($sql);

            if (!($stmt instanceof PDOStatement)) {
                return 0;
            }

            self::pdoBindParams($stmt, $params);

            if (!$stmt->execute()) {
                return 0;
            }

            $value = $stmt->fetchColumn();

            if (is_int($value) || is_float($value)) {
                return $value;
            }

            if (!is_string($value) || $value === '') {
                return 0;
            }

            if (StringUtils::isInt($value)) {
                return Cast::toInt($value);
            }

            if (StringUtils::isFloat($value)) {
                return bcadd($value, 0, 2);
            }

            return 0;
        } catch (Throwable $ex) {
            $hasError = true;
            $ex = self::wrapAsDbException($ex);
            self::freeConnection($conn, $ex);
            self::writeErrorLog($ex);
            throw $ex;
        } finally {
            if (!$hasError) {
                self::freeConnection($conn);
            }
        }
    }

    public static function deleteBySql(string $sql, array $params = []): int
    {
        return self::updateBySql($sql, $params);
    }

    public static function executeSql(string $sql, array $params = []): void
    {
        self::logSql($sql, $params);

        try {
            /* @var PdoConnection $conn */
            /* @var PDO $pdo */
            list($conn, $pdo) = self::getConnection();
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        }

        $hasError = false;

        try {
            $stmt = $pdo->prepare($sql);

            if (!($stmt instanceof PDOStatement)) {
                return;
            }

            self::pdoBindParams($stmt, $params);
            $stmt->execute();
        } catch (Throwable $ex) {
            $hasError = true;
            $ex = self::wrapAsDbException($ex);
            self::freeConnection($conn, $ex);
            self::writeErrorLog($ex);
            throw $ex;
        } finally {
            if (!$hasError) {
                self::freeConnection($conn);
            }
        }
    }

    public static function transations(callable $callback): void
    {
        try {
            if (Swoole::inCoroutineMode(true)) {
                $conn = PoolContext::getConnection(PoolContext::POOL_TYPE_DB, 2.0);
            } else {
                $conn = PdoConnection::create(self::$connectionSettings);
            }

            if (!($conn instanceof PdoConnection)) {
                throw new RuntimeException('fail to get database connection');
            }
        } catch (Throwable $ex) {
            $ex = self::wrapAsDbException($ex);
            self::writeErrorLog($ex);
            throw $ex;
        }

        /* @var PdoConnection $conn */
        $conn->inTranstionMode(true);
        TxManager::addConnection($conn);

        try {
            $conn->getRealConnection()->beginTransaction();
            $callback();
            $conn->getRealConnection()->commit();
            self::freeConnection($conn);
        } catch (Throwable $ex) {
            $conn->getRealConnection()->rollBack();
            $ex = self::wrapAsDbException($ex);
            self::freeConnection($conn, $ex);
            self::writeErrorLog($ex);
            throw $ex;
        } finally {
            $conn->inTranstionMode(false);
            TxManager::removeConnection();
        }
    }

    private static function getConnection(): array
    {
        $conn = TxManager::getConnection();

        if ($conn instanceof ConnectionInterface) {
            return [$conn, $conn->getRealConnection()];
        }

        if (Swoole::inCoroutineMode(true)) {
            $conn = PoolContext::getConnection(PoolContext::POOL_TYPE_DB, 2.0);
        } else {
            $conn = PdoConnection::create(self::$connectionSettings);
        }

        return [$conn, $conn->getRealConnection()];
    }

    private static function freeConnection(mixed $conn, ?Throwable $ex = null): void
    {
        if ($conn instanceof ConnectionInterface && !$conn->inTranstionMode()) {
            $conn->free($ex);
        }
    }

    private static function pdoBindParams(PDOStatement $stmt, array $params): void
    {
        if (empty($params)) {
            return;
        }

        foreach ($params as $i => $value) {
            if ($value === null) {
                $stmt->bindValue($i + 1, null, PDO::PARAM_NULL);
                continue;
            }

            if (is_int($value)) {
                $stmt->bindValue($i + 1, $value, PDO::PARAM_INT);
                continue;
            }

            if (is_float($value)) {
                $stmt->bindValue($i + 1, "$value");
                continue;
            }

            if (is_string($value)) {
                $stmt->bindValue($i + 1, $value);
                continue;
            }

            if (is_bool($value)) {
                $stmt->bindValue($i + 1, $value, PDO::PARAM_BOOL);
                continue;
            }

            if (is_array($value)) {
                throw new DbException(null, 'fail to bind param, param type: array');
            }

            if (is_resource($value)) {
                throw new DbException(null, 'fail to bind param, param type: resource');
            }

            if (is_object($value)) {
                throw new DbException(null, 'fail to bind param, param type: ' . $value::class);
            }
        }
    }

    private static function getTableSchemasFromCacheFile(): array
    {
        $cacheFile = FileUtils::getRealpath(self::$tableSchemasCacheFile);
        self::buildTableSchemasCacheFileDir($cacheFile);

        try {
            $schemas = include($cacheFile);
        } catch (Throwable) {
            $schemas = [];
        }

        return is_array($schemas) ? $schemas : [];
    }

    private static function writeTableSchemasToCacheFile(array $schemas): void
    {
        $cacheFile = FileUtils::getRealpath(self::$tableSchemasCacheFile);
        self::buildTableSchemasCacheFileDir($cacheFile);
        $fp = fopen($cacheFile, 'w');

        if (!is_resource($fp)) {
            return;
        }

        $sb = [
            "<?php\n",
            'return ' . var_export($schemas, true) . ";\n"
        ];

        flock($fp, LOCK_EX);
        fwrite($fp, implode('', $sb));
        flock($fp, LOCK_UN);
        fclose($fp);
    }

    private static function buildTableSchemasCacheFileDir(string $cacheFile): void
    {
        $dir = dirname($cacheFile);

        if (is_dir($dir)) {
            return;
        }

        mkdir($dir, 0755, true);
    }

    private static function wrapAsDbException(Throwable $ex): DbException
    {
        if ($ex instanceof DbException) {
            return $ex;
        }

        return new DbException(null, $ex->getMessage());
    }

    private static function logSql(string $sql, ?array $params = null): void
    {
        $logger = self::$logger;

        if (!($logger instanceof LoggerInterface) || !self::$debugLogEnabled) {
            return;
        }

        $logger->info($sql);

        if (is_array($params) && !empty($params)) {
            $logger->debug('params: ' . JsonUtils::toJson($params));
        }
    }

    private static function writeErrorLog(string|Throwable $msg): void
    {
        $logger = self::$logger;

        if (!($logger instanceof LoggerInterface)) {
            return;
        }

        if ($msg instanceof Throwable) {
            $msg = ExceptionUtils::getStackTrace($msg);
        }

        $logger->error($msg);
    }
}
