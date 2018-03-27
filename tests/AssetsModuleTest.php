<?php
namespace tests;

use Slothsoft\Farah\Module\Module;
use Slothsoft\Farah\ModuleTests\AbstractModuleTest;
use Slothsoft\Farah\Module\FarahUrl\FarahUrlAuthority;
use Slothsoft\Farah\Module\Manifest\XmlManifest;

class AssetsModuleTest extends AbstractModuleTest
{
    protected static function loadModule() : Module {
        return new Module(
            FarahUrlAuthority::createFromVendorAndModule('slothsoft', 'dbms'),
            dirname(__DIR__) . DIRECTORY_SEPARATOR . 'assets'
        );
    }
}