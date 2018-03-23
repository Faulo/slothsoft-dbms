<?php
declare(strict_types = 1);
namespace Slothsoft\DBMS;

use mysqli;
use Slothsoft\Core\Configuration\ConfigurationField;
use Slothsoft\Core\Configuration\ConfigurationRequiredException;

class Client
{
    private static function defaultAuthority() : ConfigurationField {
        static $field;
        if ($field === null) {
            $field = new ConfigurationField();
        }
        return $field;
    }
    public static function setDefaultAuthority(Authority $authority) {
        self::defaultAuthority()->setValue($authority);
    }
    public static function getDefaultAuthority() : Authority {
        return self::defaultAuthority()->getValue();
    }
    
    const CONNECTION_SERVER = 'localhost';

    const CONNECTION_CHARSET = 'utf8mb4';

    const CONNECTION_COLLATION = 'utf8mb4_unicode_ci';

    protected $sqli;

    protected $connected = false;

    protected $dbName = null;

    public function __construct()
    {}

    public function reconnect()
    {
        try {
            $authority = self::getDefaultAuthority();
        } catch(ConfigurationRequiredException $e) {
            throw new DatabaseException('Database configuration has not been set!', 0, $e);
        }
        @$this->sqli = new mysqli($authority->server, $authority->user, $authority->password);
        if ($this->sqli->connect_error) {
            $this->error();
            return false;
        }
        $this->sqli->set_charset(self::CONNECTION_CHARSET);
        if ($this->dbName) {
            $this->sqli->select_db($this->dbName);
        }
        return true;
    }

    public static function disconnect()
    {
        if ($this->connected) {
            $this->connected = false;
            $this->sqli->close();
        }
    }

    protected function connect()
    {
        return ($this->connected and $this->sqli->ping()) or ($this->reconnect() and $this->connected = true);
    }

    public function setDatabase($dbName)
    {
        $ret = null;
        $this->dbName = null;
        if ($this->connect()) {
            $ret = $this->sqli->select_db($dbName);
            $this->dbName = $dbName;
        }
        return $ret;
    }

    public function tableExists($dbName, $tableName)
    {
        $ret = null;
        if ($this->connect()) {
            $ret = $this->select('information_schema', 'tables', 'table_name', sprintf('table_schema = "%s" AND table_name = "%s"', $this->escape($dbName), $this->escape($tableName)));
            $ret = (bool) count($ret);
        }
        return $ret;
    }

    public function tableMove($oldDbName, $oldTableName, $newDbName, $newTableName)
    {
        $oldHandle = $this->get_handle($oldDbName, $oldTableName);
        $newHandle = $this->get_handle($newDbName, $newTableName);
        $sql = sprintf('RENAME TABLE %s TO %s', $oldHandle, $newHandle);
        return $this->execute($sql);
    }

    public function databaseExists($dbName)
    {
        $ret = null;
        if ($this->connect()) {
            $ret = $this->select('information_schema', 'schemata', 'schema_name', sprintf('schema_name = "%s"', $this->escape($dbName)));
            $ret = (bool) count($ret);
        }
        return $ret;
    }

    public function getDatabaseList()
    {
        $ret = null;
        if ($this->connect()) {
            $ret = $this->select('information_schema', 'schemata', 'schema_name');
        }
        return $ret;
    }

    public function getTableList($dbName)
    {
        $ret = null;
        if ($this->connect()) {
            $ret = $this->select('information_schema', 'tables', 'table_name', sprintf('table_schema = "%s"', $this->escape($dbName)));
        }
        return $ret;
    }

    public function createDatabase($dbName)
    {
        $dbHandle = $this->get_handle($dbName);
        $sql = sprintf('CREATE DATABASE IF NOT EXISTS %s', $dbHandle);
        if (! $this->execute($sql)) {
            $this->error($sql);
        }
    }

    public function deleteDatabase($dbName)
    {
        $dbHandle = $this->get_handle($dbName);
        $sql = sprintf('DROP DATABASE IF EXISTS %s', $dbHandle);
        if (! $this->execute($sql)) {
            $this->error($sql);
        }
    }

    public function createTable($dbName, $tableName, array $cols, array $keys, array $options = [])
    {
        $dbHandle = $this->get_handle($dbName, $tableName);
        $colStr = [];
        foreach ($cols as $key => $val) {
            $colStr[] = sprintf('`%s` %s', $key, $val);
        }
        $colStr = implode(', ', $colStr);
        $keyStr = [];
        foreach ($keys as $key => $val) {
            if ($key) {
                if (is_array($val)) {
                    if (! isset($val['name'])) {
                        $val['name'] = reset($val['columns']);
                    }
                    foreach ($val['columns'] as &$c) {
                        $c = sprintf('`%s`', $c);
                    }
                    unset($c);
                    $sql = sprintf('%s `%s` (%s)', $val['type'], $val['name'], implode(',', $val['columns']));
                } else {
                    $sql = sprintf('KEY `%s` (`%s`)', $val, $val);
                }
            } else {
                $sql = sprintf('PRIMARY KEY (`%s`)', $val);
            }
            $keyStr[] = $sql;
        }
        $keyStr = implode(', ', $keyStr);
        $optStr = '';
        if (isset($options['engine'])) {
            $optStr .= sprintf('ENGINE = %s', $options['engine']);
        }
        $sql = sprintf('CREATE TABLE %s ( %s , %s ) %s', $dbHandle, $colStr, $keyStr, $optStr);
        if (! $this->execute($sql)) {
            $this->error($sql);
        }
    }

    public function addIndex($dbName, $tableName, $index)
    {
        if (! is_array($index)) {
            $index = [
                'name' => $index,
                'columns' => [
                    $index
                ]
            ];
        }
        $dbHandle = $this->get_handle($dbName, $tableName);
        // $sql = sprintf('ALTER TABLE %s ADD INDEX `%s` (`%s`)', $dbHandle, $index['name'], implode('`,`', $index['columns']));
        $sql = sprintf('CREATE INDEX %s ON %s (%s)', $index['name'], $dbHandle, implode(',', $index['columns']));
        echo $sql . PHP_EOL;
        $this->execute($sql);
    }

    // SELECT $cols FROM $table WHERE ($string)
    // $cols: true => ['*'], 'col' => 'col', ['c1', 'c2'] => 'c1, c2'
    public function select($dbName, $tableName, $cols = true, $sqlString = '', $sqlSuffix = '')
    {
        $ret = null;
        $dbHandle = $this->get_handle($dbName, $tableName);
        if ($this->connect()) {
            if ($cols === true) {
                $cols = [
                    '*'
                ];
            }
            $retArr = is_array($cols);
            if (! $retArr) {
                $cols = [
                    (string) $cols
                ];
            }
            if (is_array($sqlString)) {
                $tmpArr = [];
                foreach ($sqlString as $key => $val) {
                    if ($val === null) {
                        $tmpArr[] = sprintf('`%s` IS NULL', $key);
                    } elseif (is_int($val)) {
                        $tmpArr[] = sprintf('`%s`=%d', $key, $val);
                    } elseif (is_array($val)) {
                        if (count($val)) {
                            foreach ($val as &$v) {
                                if (! is_int($v)) {
                                    $v = sprintf('"%s"', $this->escape($v));
                                }
                            }
                            unset($v);
                            $tmpArr[] = sprintf('`%s` IN (%s)', $key, implode(',', $val));
                        } else {
                            $tmpArr[] = '0';
                        }
                    } else {
                        $tmpArr[] = sprintf('`%s`="%s"', $key, $this->escape($val));
                    }
                    /*
                     * $tmpArr[] = $val === null
                     * ? sprintf('`%s` IS NULL', $key)
                     * : sprintf('`%s` = "%s"', $key, $this->escape($val));
                     * //
                     */
                }
                $sqlString = implode(' AND ', $tmpArr);
            }
            if (! strlen($sqlString)) {
                $sqlString = '1';
            }
            if (strlen($sqlSuffix)) {
                $sqlString .= ' ' . $sqlSuffix;
            }
            $sql = sprintf('SELECT %s FROM %s WHERE %s', implode(',', $cols), $dbHandle, $sqlString);
            if ($res = $this->execute($sql)) {
                if ($retArr) {
                    $ret = $res->fetch_all(MYSQLI_ASSOC);
                } else {
                    $ret = [];
                    foreach ($res as $tmp) {
                        $ret[] = current($tmp);
                    }
                    /*
                     * if ($res->num_rows > 0) {
                     * while ($tmp = $res->fetch_assoc()) {
                     * //my_dump($tmp);
                     * $ret[] = $retArr
                     * ? $tmp
                     * : current($tmp);
                     * }
                     * }
                     * //
                     */
                }
            } else {
                $this->error($sql);
            }
        }
        return $ret;
    }

    // INSERT INTO $table ($arr[key]) VALUES ($arr[val])
    public function insert($dbName, $tableName, $insertData = [], $onDuplicateData = [])
    {
        $ret = null;
        $dbHandle = $this->get_handle($dbName, $tableName);
        if ($this->connect()) {
            $keys = array_keys($insertData);
            foreach ($insertData as &$val) {
                if ($val === null) {
                    $val = 'NULL';
                } elseif (is_int($val)) {} else {
                    $val = sprintf('"%s"', $this->escape($val));
                }
                /*
                 * $val = $val === null
                 * ? 'NULL'
                 * : sprintf('"%s"', $this->escape($val));
                 * //
                 */
            }
            unset($val);
            $onDuplicateSQL = '';
            if (count($onDuplicateData)) {
                $onDuplicateSQL = sprintf(' ON DUPLICATE KEY UPDATE %s', $this->_get_update_data($onDuplicateData));
            }
            $sql = sprintf('INSERT INTO %s (`%s`) VALUES (%s)%s', $dbHandle, implode('`,`', $keys), implode(',', $insertData), $onDuplicateSQL);
            if ($this->execute($sql)) {
                $ret = $this->sqli->insert_id;
            } else {
                $this->error($sql);
            }
        }
        return $ret;
    }

    // UPDATE $table SET ($arr[key] = $arr[val]) WHERE id = $id
    public function update($dbName, $tableName, $arr = [], $id = false)
    {
        $ret = null;
        $dbHandle = $this->get_handle($dbName, $tableName);
        if ($this->connect()) {
            $sql = sprintf('UPDATE %s SET %s WHERE %s', $dbHandle, $this->_get_update_data($arr), $this->get_ids($id));
            if ($this->execute($sql)) {
                $ret = $this->sqli->affected_rows;
            } else {
                $this->error($sql);
            }
        }
        return $ret;
    }

    // DELETE FROM $table WHERE id = $id
    public function delete($dbName, $tableName, $id = false)
    {
        $ret = null;
        $dbHandle = $this->get_handle($dbName, $tableName);
        if ($this->connect()) {
            $sql = sprintf('DELETE FROM %s WHERE %s', $dbHandle, $this->get_ids($id));
            if ($this->execute($sql)) {
                $ret = $this->sqli->affected_rows;
            } else {
                $this->error($sql);
            }
        }
        return $ret;
    }

    // führt alles Mögliche aus, möglichst vermeiden ^^
    public function execute($sqlString)
    {
        if ($this->connect()) {
            Manager::_createLog($sqlString);
            return $this->sqli->query($sqlString);
        }
    }

    public function executeFile($file)
    {
        if ($sql = file_get_contents($file)) {
            return $this->sqli->multi_query($sql);
        }
    }

    public function getColumns($dbName, $tableName)
    {
        $ret = null;
        $dbHandle = $this->get_handle($dbName, $tableName);
        if ($this->connect()) {
            $sql = sprintf('SHOW COLUMNS from %s', $dbHandle);
            if ($res = $this->execute($sql)) {
                $ret = [];
                if ($res->num_rows > 0) {
                    while ($tmp = $res->fetch_assoc()) {
                        $ret[] = $tmp;
                    }
                }
            } else {
                $this->error($sql);
            }
        }
        return $ret;
    }

    public function optimize($dbName, $tableName)
    {
        $dbHandle = $this->get_handle($dbName, $tableName);
        $sql = sprintf('OPTIMIZE TABLE %s', $dbHandle);
        $res = $this->execute($sql);
        $ret = [];
        if ($res->num_rows > 0) {
            while ($tmp = $res->fetch_assoc()) {
                $ret[] = $tmp;
            }
        }
        $err = [];
        foreach ($ret as $arr) {
            if (in_array($arr['Msg_text'], [
                'OK',
                'Table is already up to date'
            ])) {
                return true;
            } else {
                $err[] = $arr['Msg_text'];
            }
        }
        throw new DatabaseException(implode(PHP_EOL, $err));
    }

    public function resetCharset($dbName, $tableName = null)
    {
        // ALTER DATABASE <database_name> CHARACTER SET utf8 COLLATE utf8_unicode_ci;
        // ALTER TABLE <table_name> DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        // ALTER TABLE <table_name> CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        $mode = null;
        if (strlen($dbName)) {
            $mode = 'db';
            if (strlen($tableName)) {
                $mode = 'table';
            }
        }
        switch ($mode) {
            case 'db':
                $sql = sprintf('ALTER DATABASE %s CHARACTER SET %s COLLATE %s', $this->get_handle(null, $dbName), self::CONNECTION_CHARSET, self::CONNECTION_COLLATION);
                $this->execute($sql);
                break;
            case 'table':
                $sql = sprintf('ALTER TABLE %s DEFAULT CHARACTER SET %s COLLATE %s', $this->get_handle($dbName, $tableName), self::CONNECTION_CHARSET, self::CONNECTION_COLLATION);
                $this->execute($sql);
                $sql = sprintf('ALTER TABLE %s CONVERT TO CHARACTER SET %s COLLATE %s', $this->get_handle($dbName, $tableName), self::CONNECTION_CHARSET, self::CONNECTION_COLLATION);
                $this->execute($sql);
                break;
        }
    }

    public function escape($string)
    {
        if ($this->connect()) {
            return $this->sqli->real_escape_string($string);
        }
        return $string;
    }

    // gibt id-string zurück
    protected function get_ids($id)
    {
        if (is_array($id)) {
            switch (count($id)) {
                case 0:
                    return '0';
                case 1:
                    return sprintf('id=%d', reset($id));
                default:
                    return sprintf('id IN (%s)', implode(',', $id));
            }
        }
        if (is_int($id)) {
            return sprintf('id=%d', $id);
        }
        if (is_bool($id)) {
            return '1';
        }
        return sprintf('id="%s"', $this->escape($id));
        /*
         * switch (true) {
         * case is_array($id):
         * return count($id) ? sprintf('id IN (%s)', implode(',', $id)) : '0';
         * case is_bool($id):
         * return '1';
         * default:
         * return sprintf('id=%d', $id);
         * }
         * //
         */
    }

    // gibt update-string zurück
    protected function _get_update_data(array $arr)
    {
        $ret = [];
        foreach ($arr as $key => $val) {
            if ($val === null) {
                $ret[] = sprintf('`%s`=NULL', $key);
            } elseif (is_int($val)) {
                $ret[] = sprintf('`%s`=%d', $key, $val);
            } else {
                $ret[] = sprintf('`%s`="%s"', $key, $this->escape($val));
            }
            /*
             * $ret[] = $val === null
             * ? sprintf('`%s` = NULL', $key)
             * : sprintf('`%s` = "%s"', $key, $this->escape($val));
             * //
             */
        }
        return implode(',', $ret);
    }

    // gibt db-handle zurück
    protected function get_handle($dbName, $tableName = null)
    {
        if ($dbName === null) {
            $dbName = $tableName;
            $tableName = null;
        }
        return $tableName === null ? sprintf('`%s`', $dbName) : sprintf('`%s`.`%s`', $dbName, $tableName);
    }

    protected function error($sql = null)
    {
        $err = '';
        if ($sql) {
            $err .= 'ERROR querying statement:' . PHP_EOL;
            $err .= $sql . PHP_EOL;
        } else {
            $err .= 'ERROR while mysqling!' . PHP_EOL;
        }
        throw new DatabaseException($err);
    }
}