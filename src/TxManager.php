<?php

namespace mgboot\databasex;

use mgboot\poolx\ConnectionInterface;
use mgboot\swoole\Swoole;

final class TxManager
{
    private static ?ConnectionInterface $curConn = null;
    private static array $connMap = [];

    private function __construct()
    {
    }

    private function __clone(): void
    {
    }

    public static function addConnection(ConnectionInterface $conn): void
    {
        $workerId = Swoole::getWorkerId();

        if ($workerId < 0) {
            $curConn = self::$curConn;

            if ($curConn instanceof ConnectionInterface) {
                $conn->free();
            } else {
                self::$curConn = $conn;
            }

            return;
        }

        if (!isset(self::$connMap["worker$workerId"])) {
            self::$connMap["worker$workerId"] = [];
        }

        $cid = Swoole::getCoroutineId();
        $curConn = self::$connMap["worker$workerId"]["cid$cid"];

        if ($curConn instanceof ConnectionInterface) {
            $conn->free();
        } else {
            self::$connMap["worker$workerId"]["cid$cid"] = $conn;
        }
    }

    public static function getConnection(): ?ConnectionInterface
    {
        $workerId = Swoole::getWorkerId();

        if ($workerId < 0) {
            return self::$curConn;
        }

        if (!is_array(self::$connMap["worker$workerId"])) {
            return null;
        }

        $cid = Swoole::getCoroutineId();
        $conn = self::$connMap["worker$workerId"]["cid$cid"];
        return $conn instanceof ConnectionInterface ? $conn : null;
    }

    public static function removeConnection(): void
    {
        $workerId = Swoole::getWorkerId();

        if ($workerId < 0) {
            self::$curConn = null;
            return;
        }

        if (!is_array(self::$connMap["worker$workerId"])) {
            return;
        }

        $cid = Swoole::getCoroutineId();
        self::$connMap["worker$workerId"]["cid$cid"] = null;
    }
}
