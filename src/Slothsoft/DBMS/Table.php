<?php
declare(strict_types = 1);
namespace Slothsoft\DBMS;

class Table
{

    protected $db;

    protected $name;

    public function __construct(Database $db, $tableName)
    {
        $this->db = $db;
        $this->name = $tableName;
    }

    public function getName()
    {
        return $this->name;
    }

    public function tableExists()
    {
        return $this->db->tableExists($this->name);
    }

    public function tableMove($newTableName, $newDbName = null)
    {
        return $this->db->tableMove($this->name, $newTableName, $newDbName);
    }

    // CREATE DATABASE
    public function createTable(array $cols, array $keys, array $options = [])
    {
        return $this->db->createTable($this->name, $cols, $keys, $options);
    }

    // SELECT $cols FROM $table WHERE ($string)
    // $cols: true => ['*'], 'col' => 'col', ['c1', 'c2'] => 'c1, c2'
    public function select($cols = true, $sqlString = '', $sqlSuffix = '')
    {
        return $this->db->select($this->name, $cols, $sqlString, $sqlSuffix);
    }

    // INSERT INTO $table ($arr[key]) VALUES ($arr[val])
    public function insert($insertData = [], $onDuplicateData = [])
    {
        return $this->db->insert($this->name, $insertData, $onDuplicateData);
    }

    // UPDATE $table SET ($arr[key] = $arr[val]) WHERE id = $id
    public function update($arr = [], $id = false)
    {
        return $this->db->update($this->name, $arr, $id);
    }

    // DELETE FROM $table WHERE id = $id
    public function delete($id = false)
    {
        return $this->db->delete($this->name, $id);
    }

    // SHOW COLUMNS
    public function getColumns()
    {
        return $this->db->getColumns($this->name);
    }

    public function optimize()
    {
        return $this->db->optimize($this->name);
    }

    public function escape($string)
    {
        return $this->db->escape($string);
    }

    public function addIndex($index)
    {
        return $this->db->addIndex($this->name, $index);
    }
}