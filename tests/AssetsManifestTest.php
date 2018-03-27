<?php
namespace tests;

use Slothsoft\Farah\ModuleTests\AbstractXmlManifestTest;
use Slothsoft\Farah\Module\Manifest\ManifestInterface;
use Slothsoft\Farah\Module\Manifest\XmlManifest;

class AssetsManifestTest extends AbstractXmlManifestTest
{
    protected static function loadManifest() : ManifestInterface {
        return new XmlManifest(dirname(__DIR__) . DIRECTORY_SEPARATOR . 'assets' . DIRECTORY_SEPARATOR . '.xml');
    }
}