<?php

include "Model.class.php";

include "functions.php";

use Think\Model;

$db_config = [
'DB_TYPE'=>'mysql',
'DB_HOST'=>'103.235.169.106',
'DB_PORT' => '3306',
'DB_NAME'=>'api',
'DB_USER'=>'api',
'DB_PWD'=>'PkwYA8Bdvx',
'DB_PREFIX'=>'vpn_',
'DB_PARAMS'=>array('persist'=>true),
];
C($db_config);

define('APP_DEBUG',false);
$r = M('project')->where(['status' =>1])->select();
$m = M('transfer');
$m->add(['groute_id' => 2]);

var_dump($r);exit('x');