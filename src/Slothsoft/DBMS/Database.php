<?php
declare(strict_types = 1);
namespace Slothsoft\DBMS;

class Database
{

    protected $client;

    protected $name;

    public function __construct(Client $client, $dbName)
    {
        $this->client = $client;
        $this->name = $dbName;
        // $this->client->setDatabase($this->name);
    }

    public function getName()
    {
        return $this->name;
    }

    public function tableExists($tableName)
    {
        return $this->client->tableExists($this->name, $tableName);
    }

    public function tableMove($oldTableName, $newTableName, $newDbName = null)
    {
        if (! $newDbName) {
            $newDbName = $this->name;
        }
        return $this->client->tableMove($this->name, $oldTableName, $newDbName, $newTableName);
    }

    public function databaseExists()
    {
        return $this->client->databaseExists($this->name);
    }

    public function getTableList()
    {
        return $this->client->getTableList($this->name);
    }

    public function getTable($tableName)
    {
        return Manager::getTable($this->name, $tableName);
    }

    // CREATE DATABASE
    public function createDatabase()
    {
        return $this->client->createDatabase($this->name);
    }

    public function deleteDatabase()
    {
        return $this->client->deleteDatabase($this->name);
    }

    // CREATE TABLE
    public function createTable($tableName, array $cols, array $keys, array $options = [])
    {
        return $this->client->createTable($this->name, $tableName, $cols, $keys, $options);
    }

    // SELECT $cols FROM $table WHERE ($string)
    // $cols: true => ['*'], 'col' => 'col', ['c1', 'c2'] => 'c1, c2'
    public function select($tableName, $cols = true, $sqlString = '', $sqlSuffix = '')
    {
        return $this->client->select($this->name, $tableName, $cols, $sqlString, $sqlSuffix);
    }

    // INSERT INTO $table ($arr[key]) VALUES ($arr[val])
    public function insert($tableName, $insertData = [], $onDuplicateData = [])
    {
        return $this->client->insert($this->name, $tableName, $insertData, $onDuplicateData);
    }

    // UPDATE $table SET ($arr[key] = $arr[val]) WHERE id = $id
    public function update($tableName, $arr = [], $id = false)
    {
        return $this->client->update($this->name, $tableName, $arr, $id);
    }

    // DELETE FROM $table WHERE id = $id
    public function delete($tableName, $id = false)
    {
        return $this->client->delete($this->name, $tableName, $id);
    }

    // SHOW COLUMNS
    public function getColumns($tableName)
    {
        return $this->client->getColumns($this->name, $tableName);
    }

    public function optimize($tableName = null)
    {
        if ($tableName) {
            return $this->client->optimize($this->name, $tableName);
        } else {
            $tableNameList = $this->getTableList();
            foreach ($tableNameList as $tableName) {
                $this->client->optimize($this->name, $tableName);
            }
        }
    }

    public function escape($string)
    {
        return $this->client->escape($string);
    }

    public function addIndex($tableName, $index)
    {
        return $this->client->addIndex($this->name, $tableName, $index);
    }

    public function resetCharset()
    {
        $this->client->resetCharset($this->name);
        $tableList = $this->getTableList();
        foreach ($tableList as $table) {
            $table = Manager::getTable($this->name, $table);
            $this->client->resetCharset($this->name, $table->getName());
        }
    }
}