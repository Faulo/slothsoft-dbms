<?php
namespace tests;

use Slothsoft\Farah\ModuleTests\XmlManifestTest;
use Slothsoft\Farah\Module\Manifest\ManifestInterface;
use Slothsoft\Farah\Module\Manifest\XmlManifest;

class AssetsManifestTest extends XmlManifestTest
{
    protected static function loadManifest() : ManifestInterface {
        return new XmlManifest(dirname(__DIR__) . DIRECTORY_SEPARATOR . 'assets' . DIRECTORY_SEPARATOR . '.xml');
    }
}