<?php
namespace Slothsoft\DBMS;

class Manager
{

    const LOG_PATH = SERVER_ROOT . DIR_LOG . 'manager.log';

    const LOG_LINELENGTH = 120;

    protected static $client;

    protected static $databaseList = [];

    protected static $tableList = [];

    public static function getClient()
    {
        if (! self::$client) {
            self::_createLog(sprintf('Manager: creating Client...'));
            self::$client = new Client();
        }
        return self::$client;
    }

    public static function getDatabase($dbName)
    {
        $dbName = mb_strtolower(trim($dbName));
        if (! isset(self::$databaseList[$dbName])) {
            
            self::_createLog(sprintf('Manager: creating Database %s...', $dbName));
            
            self::$databaseList[$dbName] = new Database(self::getClient(), $dbName);
        }
        return self::$databaseList[$dbName];
    }

    public static function getTable($dbName, $tableName)
    {
        $dbName = mb_strtolower(trim($dbName));
        $tableName = mb_strtolower(trim($tableName));
        if (! isset(self::$tableList[$dbName])) {
            self::$tableList[$dbName] = [];
        }
        if (! isset(self::$tableList[$dbName][$tableName])) {
            
            self::_createLog(sprintf('Manager: creating Table %s.%s...', $dbName, $tableName));
            
            self::$tableList[$dbName][$tableName] = new Table(self::getDatabase($dbName), $tableName);
        }
        return self::$tableList[$dbName][$tableName];
    }

    public static function cron()
    {
        $infoTable = self::getTable('information_schema', 'TABLES');
        $tableList = $infoTable->select([
            'TABLE_SCHEMA',
            'TABLE_NAME',
            'ENGINE'
        ], [
            'TABLE_TYPE' => 'BASE TABLE',
            'ENGINE' => [
                "InnoDB",
                "MyISAM"
            ]
        ]);
        // my_dump($tableList);die();
        foreach ($tableList as $table) {
            $dbName = $table['TABLE_SCHEMA'];
            $tableName = $table['TABLE_NAME'];
            $dataTable = self::getTable($dbName, $tableName);
            if ($dataTable->tableExists()) {
                echo sprintf('Optimizing %s.%s...', $dbName, $tableName);
                if ($dataTable->optimize()) {
                    echo 'OK!' . PHP_EOL;
                } else {
                    echo 'FAILURE?!?';
                    die();
                }
            }
        }
    }

    public static function _createLog($sql)
    {
        if (DBMS_MANAGER_LOG_ENABLED) {
            if (strlen($sql) > self::LOG_LINELENGTH) {
                $sql = substr($sql, 0, self::LOG_LINELENGTH) . '...';
            }
            $log = sprintf('[%s] %s%s', date(DateTimeFormatter::FORMAT_DATETIME), $sql, PHP_EOL);
            if ($handle = fopen(self::LOG_PATH, 'ab')) {
                fwrite($handle, $log);
                fclose($handle);
            }
        }
    }
}