<?php
declare(strict_types = 1);
namespace Slothsoft\DBMS;

class Authority
{
    public $server;
    public $user;
    public $password;
    
    public function __construct(string $server, string $user, string $password)
    {
        $this->server = $server;
        $this->user = $user;
        $this->password = $password;
    }
}

