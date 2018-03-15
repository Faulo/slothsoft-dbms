<?php

use Slothsoft\Farah\Module\Module;
use Slothsoft\Farah\Module\ModuleRepository;
use Slothsoft\Farah\Module\FarahUrl\FarahUrlAuthority;
use Slothsoft\Farah\Module\Manifest\XmlManifest;
use Slothsoft\Farah\Stream\FarahWrapper;

ModuleRepository::getInstance()->registerModule(
    new Module(
        FarahUrlAuthority::createFromVendorAndModule('slothsoft', 'dbms'),
        new XmlManifest(dirname(__DIR__) . DIRECTORY_SEPARATOR . 'assets.xml'),
        dirname(__DIR__) . DIRECTORY_SEPARATOR . 'assets'
    )
);