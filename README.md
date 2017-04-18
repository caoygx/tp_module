# tp_module

##model_pdo 
将tp3的model模块抽取出来了，独立放到一个文件里，使用了mysql驱动，其它项目只要include进去，就可以使用tp的model了。

```
include "Model.class.php";

include "functions.php";

use Think\Model;

$db_config = [
'DB_TYPE'=>'mysql',
'DB_HOST'=>'localhost',
'DB_PORT' => '3306',
'DB_NAME'=>'user',
'DB_USER'=>'root',
'DB_PWD'=>'123456',
'DB_PREFIX'=>'vpn_',
];
C($db_config);

define('APP_DEBUG',false);
$r = M('project')->where(['status' =>1])->select();
$m = M('user');
$m->add(['user_name' => 'aaa']);

var_dump($r);exit('x');
```