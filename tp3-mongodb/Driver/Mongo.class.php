<?php
// +----------------------------------------------------------------------
// | ThinkPHP [ WE CAN DO IT JUST THINK IT ]
// +----------------------------------------------------------------------
// | Copyright (c) 2006-2014 http://thinkphp.cn All rights reserved.
// +----------------------------------------------------------------------
// | Licensed ( http://www.apache.org/licenses/LICENSE-2.0 )
// +----------------------------------------------------------------------
// | Author: liu21st <liu21st@gmail.com>
// +----------------------------------------------------------------------

namespace Think\Db\Driver;
use Think\Db\Driver;


use MongoDB\Driver\BulkWrite;
use MongoDB\Driver\Command;
use MongoDB\Driver\Cursor;
use MongoDB\Driver\Exception\AuthenticationException;
use MongoDB\Driver\Exception\BulkWriteException;
use MongoDB\Driver\Exception\ConnectionException;
use MongoDB\Driver\Exception\InvalidArgumentException;
use MongoDB\Driver\Exception\RuntimeException;
use MongoDB\Driver\Manager;
use MongoDB\Driver\Query as MongoQuery;
use MongoDB\Driver\ReadPreference;
use MongoDB\Driver\WriteConcern;
use Think\Log;


/**
 * Mongo数据库驱动
 */
class Mongo extends Driver
{

    protected $_mongo = null; // MongoDb Object
    protected $_collection = null; // MongoCollection Object
    protected $_dbName = ''; // dbName
    protected $_collectionName = ''; // collectionName
    protected $_cursor = null; // MongoCursor Object
    protected $comparison = array('neq' => 'ne', 'ne' => 'ne', 'gt' => 'gt', 'egt' => 'gte', 'gte' => 'gte', 'lt' => 'lt', 'elt' => 'lte', 'lte' => 'lte', 'in' => 'in', 'not in' => 'nin', 'nin' => 'nin');


    protected $dbName = ''; // dbName
    /** @var string 当前SQL指令 */
    protected $queryStr = '';
    // 查询数据集类型
    protected $resultSetType = 'array';
    // 查询数据类型
    protected $typeMap = 'array';
    protected $mongo; // MongoDb Object
    protected $cursor; // MongoCursor Object

    // 监听回调
    protected static $event = [];
    /** @var PDO[] 数据库连接ID 支持多个连接 */
    protected $links = [];
    /** @var PDO 当前连接ID */
    protected $linkID;
    protected $linkRead;
    protected $linkWrite;

    // 返回或者影响记录数
    protected $numRows = 0;
    // 错误信息
    protected $error = '';
    // 查询对象
    protected $query = [];
    // 查询参数
    protected $options = [];

    protected $config = [
        // 数据库类型
        'type' => '',
        // 服务器地址
        'hostname' => 'localhost',
        // 数据库名
        'database' => 'test',
        // 用户名
        'username' => '',
        // 密码
        'password' => '',
        // 端口
        'hostport' => '27017',
        // 连接dsn
        'dsn' => '',
        // 数据库连接参数
        'params' => [],
        // 数据库编码默认采用utf8
        'charset' => 'utf8',
        // 主键名
        'pk' => '_id',
        // 数据库表前缀
        'prefix' => '',
        // 数据库调试模式
        'debug' => false,
        // 数据库部署方式:0 集中式(单一服务器),1 分布式(主从服务器)
        'deploy' => 0,
        // 数据库读写是否分离 主从式有效
        'rw_separate' => false,
        // 读写分离后 主服务器数量
        'master_num' => 1,
        // 指定从服务器序号
        'slave_no' => '',
        // 是否严格检查字段是否存在
        'fields_strict' => true,
        // 数据集返回类型
        'resultset_type' => 'array',
        // 自动写入时间戳字段
        'auto_timestamp' => false,
        // 是否需要进行SQL性能分析
        'sql_explain' => false,
        // 是否_id转换为id
        'pk_convert_id' => false,
        // typeMap
        'type_map' => ['root' => 'array', 'document' => 'array'],
        // Query对象
        'query' => '\\think\\mongo\\Query',
    ];


    // 数据库Connection对象实例
    protected $connection;
    // 数据库驱动类型
    protected $driver;
    // 当前模型类名称
    protected $model;
    // 当前数据表名称（含前缀）
    protected $table = '';
    // 当前数据表名称（不含前缀）
    protected $name = '';
    // 当前数据表主键
    protected $pk;
    // 当前数据表前缀
    protected $prefix = '';
    // 数据表信息
    protected static $info = [];

    /**
     * 架构函数 读取数据库配置信息
     * @access public
     * @param array $config 数据库配置数组
     */
    public function __construct($config = '')
    {
        /* if ( !class_exists('mongoClient') ) {
             E(L('_NOT_SUPPORT_').':Mongo');
         }*/
        if (!empty($config)) {
            $this->config = array_merge($this->config, $config);
            if (empty($this->config['params'])) {
                $this->config['params'] = array();
            }
        }
    }

    protected $conn = null;

    /**
     * 连接数据库方法
     * @access public
     */
    public function connect($config = [], $linkNum = 0)
    {

        if (!isset($this->links[$linkNum])) {
            if (empty($config)) {
                $config = $this->config;
            } else {
                $config = array_merge($this->config, $config);
            }
            $this->dbName = $config['database'];
            $this->typeMap = $config['type_map'];
            // 记录数据集返回类型
            if (isset($config['resultset_type'])) {
                $this->resultSetType = $config['resultset_type'];
            }
            if ($config['pk_convert_id'] && '_id' == $config['pk']) {
                $this->config['pk'] = 'id';
            }
            $host = 'mongodb://' . ($config['username'] ? "{$config['username']}" : '') . ($config['password'] ? ":{$config['password']}@" : '') . $config['hostname'] . ($config['hostport'] ? ":{$config['hostport']}" : '') . '/' . ($config['database'] ? "{$config['database']}" : '');
            $host = 'mongodb://localhost:27017/test';
            if ($config['debug']) {
                $startTime = microtime(true);
            }
            $this->links[$linkNum] = new Manager($host, $this->config['params']);
            if ($config['debug']) {
                // 记录数据库连接信息
                Log::record('[ DB ] CONNECT:[ UseTime:' . number_format(microtime(true) - $startTime, 6) . 's ] ' . $config['dsn'], 'sql');
            }
        }
        return $this->links[$linkNum];


        /*if ( !isset($this->linkID[$linkNum]) ) {
            if(empty($config))  $config =   $this->config;
            $host = 'mongodb://'.($config['username']?"{$config['username']}":'').($config['password']?":{$config['password']}@":'').$config['hostname'].($config['hostport']?":{$config['hostport']}":'').'/'.($config['database']?"{$config['database']}":'');
            try{
                $this->linkID[$linkNum] = new \mongoClient( $host,$this->config['params']);
            }catch (\MongoConnectionException $e){
                E($e->getmessage());
            }
        }
        return $this->linkID[$linkNum];*/
    }

    /**
     * 关闭数据库
     * @access public
     */
    public function close()
    {
        if ($this->mongo) {
            $this->mongo = null;
            $this->cursor = null;
        }
    }

    /**
     * 初始化数据库连接
     * @access protected
     * @param boolean $master 是否主服务器
     * @return void
     */
    protected function initConnect($master = true)
    {
        if (!empty($this->config['deploy'])) {
            // 采用分布式数据库
            if ($master) {
                if (!$this->linkWrite) {
                    $this->linkWrite = $this->multiConnect(true);
                }
                $this->mongo = $this->linkWrite;
            } else {
                if (!$this->linkRead) {
                    $this->linkRead = $this->multiConnect(false);
                }
                $this->mongo = $this->linkRead;
            }
        } elseif (!$this->mongo) {
            // 默认单数据库
            $this->mongo = $this->connect();
        }
    }

    /**
     * 连接分布式服务器
     * @access protected
     * @param boolean $master 主服务器
     * @return PDO
     */
    protected function multiConnect($master = false)
    {
        $_config = [];
        // 分布式数据库配置解析
        foreach (['username', 'password', 'hostname', 'hostport', 'database', 'dsn', 'charset'] as $name) {
            $_config[$name] = explode(',', $this->config[$name]);
        }

        // 主服务器序号
        $m = floor(mt_rand(0, $this->config['master_num'] - 1));

        if ($this->config['rw_separate']) {
            // 主从式采用读写分离
            if ($master) // 主服务器写入
            {
                $r = $m;
            } elseif (is_numeric($this->config['slave_no'])) {
                // 指定服务器读
                $r = $this->config['slave_no'];
            } else {
                // 读操作连接从服务器 每次随机连接的数据库
                $r = floor(mt_rand($this->config['master_num'], count($_config['hostname']) - 1));
            }
        } else {
            // 读写操作不区分服务器 每次随机连接的数据库
            $r = floor(mt_rand(0, count($_config['hostname']) - 1));
        }
        $dbConfig = [];
        foreach (['username', 'password', 'hostname', 'hostport', 'database', 'dsn', 'charset'] as $name) {
            $dbConfig[$name] = isset($_config[$name][$r]) ? $_config[$name][$r] : $_config[$name][0];
        }
        return $this->connect($dbConfig, $r);
    }

    /**
     * 析构方法
     * @access public
     */
    public function __destruct()
    {
        // 释放查询
        $this->free();

        // 关闭连接
        $this->close();
    }

    /**
     * 切换当前操作的Db和Collection
     * @access public
     * @param string $collection collection
     * @param string $db db
     * @param boolean $master 是否主服务器
     * @return void
     */
    public function switchCollection($collection, $db = '', $master = true)
    {
        // 当前没有连接 则首先进行数据库连接
        if (!$this->_linkID) $this->initConnect($master);
        /* try{
             if(!empty($db)) { // 传人Db则切换数据库
                 // 当前MongoDb对象
                 $this->_dbName  =  $db;
                 $this->_mongo = $this->_linkID->selectDb($db);
             }
             // 当前MongoCollection对象
             if($this->config['debug']) {
                 $this->queryStr   =  $this->_dbName.'.getCollection('.$collection.')';
             }
             if($this->_collectionName != $collection) {
                 $this->queryTimes++;
                 N('db_query',1); // 兼容代码
                 $this->debug(true);
                 $this->_collection =  $this->_mongo->selectCollection($collection);
                 $this->debug(false);
                 $this->_collectionName  = $collection; // 记录当前Collection名称
             }
         }catch (MongoException $e){
             E($e->getMessage());
         }*/
    }

    /**
     * 释放查询结果
     * @access public
     */
    public function free()
    {
        $this->_cursor = null;
    }


    /**
     * 获得数据集
     * @access protected
     * @param bool|string $class true 返回Mongo cursor对象 字符串用于指定返回的类名
     * @param string|array $typeMap 指定返回的typeMap
     * @return mixed
     */
    public function getResult($class = '', $typeMap = null)
    {


        if (true === $class) {
            return $this->cursor;
        }
        // 设置结果数据类型
        if (is_null($typeMap)) {
            $typeMap = $this->typeMap;
        }
        $typeMap = !is_array($typeMap) ? ['root' => $typeMap] : $typeMap;
        $this->cursor->setTypeMap($typeMap);

        // 获取数据集
        $result = $this->cursor->toArray();
        if ($this->getConfig('pk_convert_id')) {
            // 转换ObjectID 字段
            foreach ($result as &$data) {
                $this->convertObjectID($data);
            }
        }
        $this->numRows = count($result);
        if (!empty($class)) {
            // 返回指定数据集对象类
            $result = new $class($result);
        } elseif ('collection' == $this->resultSetType) {
            // 返回数据集Collection对象
            $result = new Collection($result);
        }
        return $result;
    }

    /**
     * ObjectID处理
     * @access public
     * @param array $data
     * @return void
     */
    private function convertObjectID(&$data)
    {
        if (isset($data['_id'])) {
            $data['id'] = $data['_id']->__toString();
            unset($data['_id']);
        }
    }

    /**
     * 执行命令
     * @access public
     * @param array $command 指令
     * @return array
     */
    public function command2($command = array(), $options = array())
    {
        $cache = isset($options['cache']) ? $options['cache'] : false;
        if ($cache) { // 查询缓存检测
            $key = is_string($cache['key']) ? $cache['key'] : md5(serialize($command));
            $value = S($key, '', '', $cache['type']);
            if (false !== $value) {
                return $value;
            }
        }
        N('db_write', 1); // 兼容代码
        $this->executeTimes++;
        try {
            if ($this->config['debug']) {
                $this->queryStr = $this->_dbName . '.' . $this->_collectionName . '.runCommand(';
                $this->queryStr .= json_encode($command);
                $this->queryStr .= ')';
            }
            $this->debug(true);
            $result = $this->_mongo->command($command);
            $this->debug(false);

            if ($cache && $result['ok']) { // 查询缓存写入
                S($key, $result, $cache['expire'], $cache['type']);
            }
            return $result;
        } catch (\MongoCursorException $e) {
            E($e->getMessage());
        }
    }

    /**
     * 执行语句
     * @access public
     * @param string $code sql指令
     * @param array $args 参数
     * @return mixed
     */
    public function execute2($code, $args = array())
    {
        $this->executeTimes++;
        N('db_write', 1); // 兼容代码
        $this->debug(true);
        $this->queryStr = 'execute:' . $code;
        $result = $this->_mongo->execute($code, $args);
        $this->debug(false);
        if ($result['ok']) {
            return $result['retval'];
        } else {
            E($result['errmsg']);
        }
    }

    /**
     * 关闭数据库
     * @access public
     */
    public function close2()
    {
        if ($this->_linkID) {
            $this->_linkID->close();
            $this->_linkID = null;
            $this->_mongo = null;
            $this->_collection = null;
            $this->_cursor = null;
        }
    }

    /**
     * 数据库错误信息
     * @access public
     * @return string
     */
    public function error()
    {
        $this->error = $this->_mongo->lastError();
        trace($this->error, '', 'ERR');
        return $this->error;
    }

    /**
     * 插入记录
     * @access public
     * @param mixed $data 数据
     * @param array $options 参数表达式
     * @param boolean $replace 是否replace
     * @return false | integer
     */
    public function insert2($data, $options = array(), $replace = false)
    {

        if (isset($options['table'])) {
            $this->switchCollection($options['table']);
        }
        $query = new Query($this->conn);
        $query->setTable($options['table']);
        return $query->insert($data);
        /* $this->model  =   $options['model'];
         $this->executeTimes++;
         N('db_write',1); // 兼容代码
         if($this->config['debug']) {
             $this->queryStr   =  $this->_dbName.'.'.$this->_collectionName.'.insert(';
             $this->queryStr   .= $data?json_encode($data):'{}';
             $this->queryStr   .= ')';
         }
         try{
             $this->debug(true);
             $result =  $replace?   $this->_collection->save($data):  $this->_collection->insert($data);
             $this->debug(false);
             if($result) {
                $_id    = $data['_id'];
                 if(is_object($_id)) {
                     $_id = $_id->__toString();
                 }
                $this->lastInsID    = $_id;
             }
             return $result;
         } catch (\MongoCursorException $e) {
             E($e->getMessage());
         }*/
    }

    /**
     * 插入多条记录
     * @access public
     * @param array $dataList 数据
     * @param array $options 参数表达式
     * @return bool
     */
    public function insertAll2($dataList, $options = array())
    {
        if (isset($options['table'])) {
            $this->switchCollection($options['table']);
        }
        $this->model = $options['model'];
        $this->executeTimes++;
        N('db_write', 1); // 兼容代码
        try {
            $this->debug(true);
            $result = $this->_collection->batchInsert($dataList);
            $this->debug(false);
            return $result;
        } catch (\MongoCursorException $e) {
            E($e->getMessage());
        }
    }

    /**
     * 生成下一条记录ID 用于自增非MongoId主键
     * @access public
     * @param string $pk 主键名
     * @return integer
     */
    public function getMongoNextId($pk)
    {
        if ($this->config['debug']) {
            $this->queryStr = $this->_dbName . '.' . $this->_collectionName . '.find({},{' . $pk . ':1}).sort({' . $pk . ':-1}).limit(1)';
        }
        try {
            $this->debug(true);
            $result = $this->_collection->find(array(), array($pk => 1))->sort(array($pk => -1))->limit(1);
            $this->debug(false);
        } catch (\MongoCursorException $e) {
            E($e->getMessage());
        }
        $data = $result->getNext();
        return isset($data[$pk]) ? $data[$pk] + 1 : 1;
    }

    /**
     * 更新记录
     * @access public
     * @param mixed $data 数据
     * @param array $options 表达式
     * @return bool
     */
    public function update2($data, $options)
    {
        if (isset($options['table'])) {
            $this->switchCollection($options['table']);
        }
        $this->executeTimes++;
        N('db_write', 1); // 兼容代码
        $this->model = $options['model'];
        $query = $this->parseWhere(isset($options['where']) ? $options['where'] : array());
        $set = $this->parseSet($data);
        if ($this->config['debug']) {
            $this->queryStr = $this->_dbName . '.' . $this->_collectionName . '.update(';
            $this->queryStr .= $query ? json_encode($query) : '{}';
            $this->queryStr .= ',' . json_encode($set) . ')';
        }
        try {
            $this->debug(true);
            if (isset($options['limit']) && $options['limit'] == 1) {
                $multiple = array("multiple" => false);
            } else {
                $multiple = array("multiple" => true);
            }
            $result = $this->_collection->update($query, $set, $multiple);
            $this->debug(false);
            return $result;
        } catch (\MongoCursorException $e) {
            E($e->getMessage());
        }
    }

    /**
     * 删除记录
     * @access public
     * @param array $options 表达式
     * @return false | integer
     */
    public function delete2($options = array())
    {
        if (isset($options['table'])) {
            $this->switchCollection($options['table']);
        }
        $query = $this->parseWhere(isset($options['where']) ? $options['where'] : array());
        $this->model = $options['model'];
        $this->executeTimes++;
        N('db_write', 1); // 兼容代码
        if ($this->config['debug']) {
            $this->queryStr = $this->_dbName . '.' . $this->_collectionName . '.remove(' . json_encode($query) . ')';
        }
        try {
            $this->debug(true);
            $result = $this->_collection->remove($query);
            $this->debug(false);
            return $result;
        } catch (\MongoCursorException $e) {
            E($e->getMessage());
        }
    }

    /**
     * 清空记录
     * @access public
     * @param array $options 表达式
     * @return false | integer
     */
    public function clear($options = array())
    {
        if (isset($options['table'])) {
            $this->switchCollection($options['table']);
        }
        $this->model = $options['model'];
        $this->executeTimes++;
        N('db_write', 1); // 兼容代码
        if ($this->config['debug']) {
            $this->queryStr = $this->_dbName . '.' . $this->_collectionName . '.remove({})';
        }
        try {
            $this->debug(true);
            $result = $this->_collection->drop();
            $this->debug(false);
            return $result;
        } catch (\MongoCursorException $e) {
            E($e->getMessage());
        }
    }


    /**
     * 查找某个记录
     * @access public
     * @param array $options 表达式
     * @return array
     */
    public function find2($options = array())
    {

        if (isset($options['table'])) {
            $this->switchCollection($options['table']);
        }
        $query = new Query($this->conn);
        $query->options($options);
        //$query->setTable($options['table']);
        return $query->find();

        /*$options['limit'] = 1;
        $find = $this->select($options);
        return array_shift($find);*/
    }

    /**
     * 统计记录数
     * @access public
     * @param array $options 表达式
     * @return iterator
     */
    public function count2($options = array())
    {
        if (isset($options['table'])) {
            $this->switchCollection($options['table'], '', false);
        }
        $this->model = $options['model'];
        $this->queryTimes++;
        N('db_query', 1); // 兼容代码
        $query = $this->parseWhere(isset($options['where']) ? $options['where'] : array());
        if ($this->config['debug']) {
            $this->queryStr = $this->_dbName . '.' . $this->_collectionName;
            $this->queryStr .= $query ? '.find(' . json_encode($query) . ')' : '';
            $this->queryStr .= '.count()';
        }
        try {
            $this->debug(true);
            $count = $this->_collection->count($query);
            $this->debug(false);
            return $count;
        } catch (\MongoCursorException $e) {
            E($e->getMessage());
        }
    }

    public function group($keys, $initial, $reduce, $options = array())
    {
        if (isset($options['table']) && $this->_collectionName != $options['table']) {
            $this->switchCollection($options['table'], '', false);
        }

        $cache = isset($options['cache']) ? $options['cache'] : false;
        if ($cache) {
            $key = is_string($cache['key']) ? $cache['key'] : md5(serialize($options));
            $value = S($key, '', '', $cache['type']);
            if (false !== $value) {
                return $value;
            }
        }

        $this->model = $options['model'];
        $this->queryTimes++;
        N('db_query', 1); // 兼容代码
        $query = $this->parseWhere(isset($options['where']) ? $options['where'] : array());

        if ($this->config['debug']) {
            $this->queryStr = $this->_dbName . '.' . $this->_collectionName . '.group({key:' . json_encode($keys) . ',cond:' .
                json_encode($options['condition']) . ',reduce:' .
                json_encode($reduce) . ',initial:' .
                json_encode($initial) . '})';
        }
        try {
            $this->debug(true);
            $option = array('condition' => $options['condition'], 'finalize' => $options['finalize'], 'maxTimeMS' => $options['maxTimeMS']);
            $group = $this->_collection->group($keys, $initial, $reduce, $options);
            $this->debug(false);

            if ($cache && $group['ok'])
                S($key, $group, $cache['expire'], $cache['type']);

            return $group;
        } catch (\MongoCursorException $e) {
            E($e->getMessage());
        }
    }

    /**
     * 取得数据表的字段信息
     * @access public
     * @return array
     */
    public function getFields($collection = '')
    {
        return false;
        if (!empty($collection) && $collection != $this->_collectionName) {
            $this->switchCollection($collection, '', false);
        }
        $this->queryTimes++;
        N('db_query', 1); // 兼容代码
        if ($this->config['debug']) {
            $this->queryStr = $this->_dbName . '.' . $this->_collectionName . '.findOne()';
        }
        try {
            $this->debug(true);
            $result = $this->_collection->findOne();
            $this->debug(false);
        } catch (\MongoCursorException $e) {
            E($e->getMessage());
        }
        if ($result) { // 存在数据则分析字段
            $info = array();
            foreach ($result as $key => $val) {
                $info[$key] = array(
                    'name' => $key,
                    'type' => getType($val),
                );
            }
            return $info;
        }
        // 暂时没有数据 返回false
        return false;
    }

    /**
     * 取得当前数据库的collection信息
     * @access public
     */
    public function getTables()
    {
        if ($this->config['debug']) {
            $this->queryStr = $this->_dbName . '.getCollenctionNames()';
        }
        $this->queryTimes++;
        N('db_query', 1); // 兼容代码
        $this->debug(true);
        $list = $this->_mongo->listCollections();
        $this->debug(false);
        $info = array();
        foreach ($list as $collection) {
            $info[] = $collection->getName();
        }
        return $info;
    }

    /**
     * 取得当前数据库的对象
     * @access public
     * @return object mongoClient
     */
    public function getDB()
    {
        return $this->_mongo;
    }

    /**
     * 取得当前集合的对象
     * @access public
     * @return object MongoCollection
     */
    public function getCollection()
    {
        return $this->_collection;
    }

    /**
     * set分析
     * @access protected
     * @param array $data
     * @return string
     */
    protected function parseSet2($data)
    {
        $result = array();
        foreach ($data as $key => $val) {
            if (is_array($val)) {
                switch ($val[0]) {
                    case 'inc':
                        $result['$inc'][$key] = (int)$val[1];
                        break;
                    case 'set':
                    case 'unset':
                    case 'push':
                    case 'pushall':
                    case 'addtoset':
                    case 'pop':
                    case 'pull':
                    case 'pullall':
                        $result['$' . $val[0]][$key] = $val[1];
                        break;
                    default:
                        $result['$set'][$key] = $val;
                }
            } else {
                $result['$set'][$key] = $val;
            }
        }
        return $result;
    }

    /**
     * order分析
     * @access protected
     * @param mixed $order
     * @return array
     */
    protected function parseOrder($order)
    {
        if (is_string($order)) {
            $array = explode(',', $order);
            $order = array();
            foreach ($array as $key => $val) {
                $arr = explode(' ', trim($val));
                if (isset($arr[1])) {
                    $arr[1] = $arr[1] == 'asc' ? 1 : -1;
                } else {
                    $arr[1] = 1;
                }
                $order[$arr[0]] = $arr[1];
            }
        }
        return $order;
    }

    /**
     * limit分析
     * @access protected
     * @param mixed $limit
     * @return array
     */
    protected function parseLimit($limit)
    {
        if (strpos($limit, ',')) {
            $array = explode(',', $limit);
        } else {
            $array = array(0, $limit);
        }
        return $array;
    }

    /**
     * field分析
     * @access protected
     * @param mixed $fields
     * @return array
     */
    public function parseField($fields)
    {
        if (empty($fields)) {
            $fields = array();
        }
        if (is_string($fields)) {
            $_fields = explode(',', $fields);
            $fields = array();
            foreach ($_fields as $f)
                $fields[$f] = true;
        } elseif (is_array($fields)) {
            $_fields = $fields;
            $fields = array();
            foreach ($_fields as $f => $v) {
                if (is_numeric($f))
                    $fields[$v] = true;
                else
                    $fields[$f] = $v ? true : false;
            }
        }
        return $fields;
    }

    /**
     * where分析
     * @access protected
     * @param mixed $where
     * @return array
     */
    public function parseWhere2($where)
    {
        $query = array();
        $return = array();
        $_logic = '$and';
        if (isset($where['_logic'])) {
            $where['_logic'] = strtolower($where['_logic']);
            $_logic = in_array($where['_logic'], array('or', 'xor', 'nor', 'and')) ? '$' . $where['_logic'] : $_logic;
            unset($where['_logic']);
        }
        foreach ($where as $key => $val) {
            if ('_id' != $key && 0 === strpos($key, '_')) {
                // 解析特殊条件表达式
                $parse = $this->parseThinkWhere($key, $val);
                $query = array_merge($query, $parse);
            } else {
                // 查询字段的安全过滤
                if (!preg_match('/^[A-Z_\|\&\-.a-z0-9]+$/', trim($key))) {
                    E(L('_ERROR_QUERY_') . ':' . $key);
                }
                $key = trim($key);
                if (strpos($key, '|')) {
                    $array = explode('|', $key);
                    $str = array();
                    foreach ($array as $k) {
                        $str[] = $this->parseWhereItem($k, $val);
                    }
                    $query['$or'] = $str;
                } elseif (strpos($key, '&')) {
                    $array = explode('&', $key);
                    $str = array();
                    foreach ($array as $k) {
                        $str[] = $this->parseWhereItem($k, $val);
                    }
                    $query = array_merge($query, $str);
                } else {
                    $str = $this->parseWhereItem($key, $val);
                    $query = array_merge($query, $str);
                }
            }
        }
        if ($_logic == '$and')
            return $query;

        foreach ($query as $key => $val)
            $return[$_logic][] = array($key => $val);

        return $return;
    }

    /**
     * 特殊条件分析
     * @access protected
     * @param string $key
     * @param mixed $val
     * @return string
     */
    protected function parseThinkWhere($key, $val)
    {
        $query = array();
        $_logic = array('or', 'xor', 'nor', 'and');

        switch ($key) {
            case '_query': // 字符串模式查询条件
                parse_str($val, $query);
                if (isset($query['_logic']) && strtolower($query['_logic']) == 'or') {
                    unset($query['_logic']);
                    $query['$or'] = $query;
                }
                break;
            case '_complex': // 子查询模式查询条件
                $__logic = strtolower($val['_logic']);
                if (isset($val['_logic']) && in_array($__logic, $_logic)) {
                    unset($val['_logic']);
                    $query['$' . $__logic] = $val;
                }
                break;
            case '_string':// MongoCode查询
                $query['$where'] = new \MongoCode($val);
                break;
        }
        //兼容 MongoClient OR条件查询方法
        if (isset($query['$or']) && !is_array(current($query['$or']))) {
            $val = array();
            foreach ($query['$or'] as $k => $v)
                $val[] = array($k => $v);
            $query['$or'] = $val;
        }
        return $query;
    }

    /**
     * where子单元分析
     * @access protected
     * @param string $key
     * @param mixed $val
     * @return array
     */
    protected function parseWhereItem2($key, $val)
    {
        $query = array();
        if (is_array($val)) {
            if (is_string($val[0])) {
                $con = strtolower($val[0]);
                if (in_array($con, array('neq', 'ne', 'gt', 'egt', 'gte', 'lt', 'lte', 'elt'))) { // 比较运算
                    $k = '$' . $this->comparison[$con];
                    $query[$key] = array($k => $val[1]);
                } elseif ('like' == $con) { // 模糊查询 采用正则方式
                    $query[$key] = new \MongoRegex("/" . $val[1] . "/");
                } elseif ('mod' == $con) { // mod 查询
                    $query[$key] = array('$mod' => $val[1]);
                } elseif ('regex' == $con) { // 正则查询
                    $query[$key] = new \MongoRegex($val[1]);
                } elseif (in_array($con, array('in', 'nin', 'not in'))) { // IN NIN 运算
                    $data = is_string($val[1]) ? explode(',', $val[1]) : $val[1];
                    $k = '$' . $this->comparison[$con];
                    $query[$key] = array($k => $data);
                } elseif ('all' == $con) { // 满足所有指定条件
                    $data = is_string($val[1]) ? explode(',', $val[1]) : $val[1];
                    $query[$key] = array('$all' => $data);
                } elseif ('between' == $con) { // BETWEEN运算
                    $data = is_string($val[1]) ? explode(',', $val[1]) : $val[1];
                    $query[$key] = array('$gte' => $data[0], '$lte' => $data[1]);
                } elseif ('not between' == $con) {
                    $data = is_string($val[1]) ? explode(',', $val[1]) : $val[1];
                    $query[$key] = array('$lt' => $data[0], '$gt' => $data[1]);
                } elseif ('exp' == $con) { // 表达式查询
                    $query['$where'] = new \MongoCode($val[1]);
                } elseif ('exists' == $con) { // 字段是否存在
                    $query[$key] = array('$exists' => (bool)$val[1]);
                } elseif ('size' == $con) { // 限制属性大小
                    $query[$key] = array('$size' => intval($val[1]));
                } elseif ('type' == $con) { // 限制字段类型 1 浮点型 2 字符型 3 对象或者MongoDBRef 5 MongoBinData 7 MongoId 8 布尔型 9 MongoDate 10 NULL 15 MongoCode 16 32位整型 17 MongoTimestamp 18 MongoInt64 如果是数组的话判断元素的类型
                    $query[$key] = array('$type' => intval($val[1]));
                } else {
                    $query[$key] = $val;
                }
                return $query;
            }
        }
        $query[$key] = $val;
        return $query;
    }





    /**
     * 数据库日志记录（仅供参考）
     * @access public
     * @param string $type 类型
     * @param mixed  $data 数据
     * @param array  $options 参数
     * @return void
     */
    public function log($type, $data, $options = [])
    {
        if (!$this->config['debug']) {
            return;
        }
        if (is_array($data)) {
            array_walk_recursive($data, function (&$value) {
                if ($value instanceof ObjectID) {
                    $value = $value->__toString();
                }
            });
        }
        switch (strtolower($type)) {
            case 'find':
                $this->queryStr = $type . '(' . ($data ? json_encode($data) : '') . ')';
                if (isset($options['sort'])) {
                    $this->queryStr .= '.sort(' . json_encode($options['sort']) . ')';
                }
                if (isset($options['limit'])) {
                    $this->queryStr .= '.limit(' . $options['limit'] . ')';
                }
                $this->queryStr .= ';';
                break;
            case 'insert':
            case 'remove':
                $this->queryStr = $type . '(' . ($data ? json_encode($data) : '') . ');';
                break;
            case 'update':
                $this->queryStr = $type . '(' . json_encode($options) . ',' . json_encode($data) . ');';
                break;
            case 'cmd':
                $this->queryStr = $data . '(' . json_encode($options) . ');';
                break;
        }
        $this->options = $options;
    }







































































































    //================================================== query ================================================

    /**
     * 获取当前的数据库Connection对象
     * @access public
     * @return Connection
     */
    public function getConnection()
    {
        return $this->connection;
    }


    /**
     * 指定默认的数据表名（不含前缀）
     * @access public
     * @param string $name
     * @return $this
     */
    public function name($name)
    {
        $this->name = $name;
        return $this;
    }

    /**
     * 指定默认数据表名（含前缀）
     * @access public
     * @param string $table 表名
     * @return $this
     */
    public function setTable($table)
    {
        $this->table = $table;
        return $this;
    }

    /**
     * 得到当前或者指定名称的数据表
     * @access public
     * @param string $name
     * @return string
     */
    public function getTable($name = '')
    {
        if ($name || empty($this->table)) {
            $name = $name ?: $this->name;
            $tableName = $this->prefix;
            if ($name) {
                $tableName .= Loader::parseName($name);
            }
        } else {
            $tableName = $this->table;
        }
        return $tableName;
    }

    /**
     * 指定数据表主键
     * @access public
     * @param string $pk 主键
     * @return $this
     */
    public function pk($pk)
    {
        $this->pk = $pk;
        return $this;
    }

    /**
     * 将SQL语句中的__TABLE_NAME__字符串替换成带前缀的表名（小写）
     * @access public
     * @param string $sql sql语句
     * @return string
     */
    public function parseSqlTable($sql)
    {
        if (false !== strpos($sql, '__')) {
            $prefix = $this->prefix;
            $sql = preg_replace_callback("/__([A-Z0-9_-]+)__/sU", function ($match) use ($prefix) {
                return $prefix . strtolower($match[1]);
            }, $sql);
        }
        return $sql;
    }


    /**
     * 执行查询
     * @access public
     * @param string $namespace 当前查询的collection
     * @param MongoQuery $query 查询对象
     * @param ReadPreference $readPreference readPreference
     * @param string|bool $class 返回的数据集类型
     * @param string|array $typeMap 指定返回的typeMap
     * @return mixed
     * @throws AuthenticationException
     * @throws InvalidArgumentException
     * @throws ConnectionException
     * @throws RuntimeException
     */
    public function query($namespace, MongoQuery $query, ReadPreference $readPreference = null, $class = false, $typeMap = null)
    {
        $this->initConnect(false);
        $this->queryTimes++;

        if (false === strpos($namespace, '.')) {
            $namespace = $this->dbName . '.' . $namespace;
        }
        if ($this->config['debug'] && !empty($this->queryStr)) {
            // 记录执行指令
            $this->queryStr = 'db' . strstr($namespace, '.') . '.' . $this->queryStr;
        }
        $this->debug(true);

        $this->cursor = $this->mongo->executeQuery($namespace, $query, $readPreference);


        $this->debug(true);
        return $this->getResult($class, $typeMap);
    }


    /**
     * 执行指令
     * @access public
     * @param Command $command 指令
     * @param string $dbName 当前数据库名
     * @param ReadPreference $readPreference readPreference
     * @param string|bool $class 返回的数据集类型
     * @param string|array $typeMap 指定返回的typeMap
     * @return mixed
     * @throws AuthenticationException
     * @throws InvalidArgumentException
     * @throws ConnectionException
     * @throws RuntimeException
     */
    public function command(Command $command, $dbName = '', ReadPreference $readPreference = null, $class = false, $typeMap = null)
    {
        $this->initConnect(false);
        Db::$queryTimes++;

        $this->debug(true);
        $dbName = $dbName ?: $this->dbName;
        if ($this->config['debug'] && !empty($this->queryStr)) {
            $this->queryStr = 'db.' . $dbName . '.' . $this->queryStr;
        }
        $this->cursor = $this->mongo->executeCommand($dbName, $command, $readPreference);
        $this->debug(false);
        return $this->getResult($class, $typeMap);

    }


    /**
     * 执行写操作
     * @access public
     * @param string $namespace
     * @param BulkWrite $bulk
     * @param WriteConcern $writeConcern
     *
     * @return WriteResult
     * @throws AuthenticationException
     * @throws InvalidArgumentException
     * @throws ConnectionException
     * @throws RuntimeException
     * @throws BulkWriteException
     */
    public function execute($namespace, BulkWrite $bulk, WriteConcern $writeConcern = null)
    {
        $this->initConnect(true);
        //Db::$executeTimes++;
        if (false === strpos($namespace, '.')) {
            $namespace = $this->dbName . '.' . $namespace;
        }
        if ($this->config['debug'] && !empty($this->queryStr)) {
            // 记录执行指令
            $this->queryStr = 'db' . strstr($namespace, '.') . '.' . $this->queryStr;
        }
        $this->debug(true);
        //$namespace = $namespace."think_user";
        $writeResult = $this->mongo->executeBulkWrite($namespace, $bulk, $writeConcern);
        $this->debug(false);
        $this->numRows = $writeResult->getMatchedCount();
        return $writeResult;
    }

    /**
     * 获取最近插入的ID
     * @access public
     * @return string
     */
    public function getLastInsID()
    {
        $id = $this->builder_getLastInsID();
        if ($id instanceof ObjectID) {
            $id = $id->__toString();
        }
        return $id;
    }

    /**
     * 获取最近一次执行的指令
     * @access public
     * @return string
     */
    public function getLastSql()
    {
        return $this->getQueryStr();
    }

    /**
     * 获取执行的指令
     * @access public
     * @return string
     */
    public function getQueryStr()
    {
        return $this->queryStr;
    }


    /**
     * 获取数据库的配置参数
     * @access public
     * @param string $config 配置名称
     * @return mixed
     */
    public function getConfig($config = '')
    {
        return $config ? $this->config[$config] : $this->config;
    }

    /**
     * 得到某个字段的值
     * @access public
     * @param string $field 字段名
     * @param mixed $default 默认值
     * @return mixed
     */
    public function value($field, $default = null)
    {
        $result = null;
        if (!empty($this->options['cache'])) {
            // 判断查询缓存
            $cache = $this->options['cache'];
            if (empty($this->options['table'])) {
                $this->options['table'] = $this->getTable();
            }
            $key = is_string($cache['key']) ? $cache['key'] : md5($field . serialize($this->options));
            $result = Cache::get($key);
        }
        if (!$result) {
            if (isset($this->options['field'])) {
                unset($this->options['field']);
            }
            $cursor = $this->field($field)->fetchCursor(true)->find();
            $cursor->setTypeMap(['root' => 'array']);
            $resultSet = $cursor->toArray();
            $data = isset($resultSet[0]) ? $resultSet[0] : null;
            $result = $data[$field];
            if (isset($cache)) {
                // 缓存数据
                Cache::set($key, $result, $cache['expire']);
            }
        } else {
            // 清空查询条件
            $this->options = [];
        }
        return !is_null($result) ? $result : $default;
    }

    /**
     * 得到某个列的数组
     * @access public
     * @param string $field 字段名 多个字段用逗号分隔
     * @param string $key 索引
     * @return array
     */
    public function column($field, $key = '')
    {
        $result = false;
        if (!empty($this->options['cache'])) {
            // 判断查询缓存
            $cache = $this->options['cache'];
            if (empty($this->options['table'])) {
                $this->options['table'] = $this->getTable();
            }
            $guid = is_string($cache['key']) ? $cache['key'] : md5($field . serialize($this->options));
            $result = Cache::get($guid);
        }
        if (!$result) {
            if (isset($this->options['field'])) {
                unset($this->options['field']);
            }
            if ($key && '*' != $field) {
                $field = $key . ',' . $field;
            }
            $cursor = $this->field($field)->fetchCursor(true)->select();
            $cursor->setTypeMap(['root' => 'array']);
            $resultSet = $cursor->toArray();
            if ($resultSet) {
                $fields = array_keys($resultSet[0]);
                $count = count($fields);
                $key1 = array_shift($fields);
                $key2 = $fields ? array_shift($fields) : '';
                $key = $key ?: $key1;
                foreach ($resultSet as $val) {
                    $name = $val[$key];
                    if ($name instanceof ObjectID) {
                        $name = $name->__toString();
                    }
                    if (2 == $count) {
                        $result[$name] = $val[$key2];
                    } elseif (1 == $count) {
                        $result[$name] = $val[$key1];
                    } else {
                        $result[$name] = $val;
                    }
                }
            } else {
                $result = [];
            }

            if (isset($cache) && isset($guid)) {
                // 缓存数据
                Cache::set($guid, $result, $cache['expire']);
            }
        } else {
            // 清空查询条件
            $this->options = [];
        }
        return $result;
    }

    /**
     * 执行command
     * @access public
     * @param string|array|object $command 指令
     * @param mixed $extra 额外参数
     * @param string $db 数据库名
     * @return array
     */
    public function cmd($command, $extra = null, $db = null)
    {
        if (is_array($command) || is_object($command)) {
            if ($this->getConfig('debug')) {
                $this->log('cmd', 'cmd', $command);
            }
            // 直接创建Command对象
            $command = new Command($command);
        } else {
            // 调用Builder封装的Command对象
            $options = $this->parseExpress();
            $cmd = "builder_" . $command;
            $command = $this->$cmd($options, $extra);
        }
        return $this->command($command, $db);
    }

    /**
     * 指定distinct查询
     * @access public
     * @param string $field 字段名
     * @return array
     */
    public function distinct($field)
    {
        $result = $this->cmd('distinct', $field);
        return $result[0]['values'];
    }

    /**
     * 获取数据库的所有collection
     * @access public
     * @param string $db 数据库名称 留空为当前数据库
     * @throws Exception
     */
    public function listCollections($db = '')
    {
        $cursor = $this->cmd('listCollections', null, $db);
        $result = [];
        foreach ($cursor as $collection) {
            $result[] = $collection['name'];
        }
        return $result;
    }

    /**
     * COUNT查询
     * @access public
     * @return integer
     */
    public function count()
    {
        $result = $this->cmd('count');
        return $result[0]['n'];
    }

    /**
     * 设置记录的某个字段值
     * 支持使用数据库字段和方法
     * @access public
     * @param string|array $field 字段名
     * @param mixed $value 字段值
     * @return integer
     */
    public function setField($field, $value = '')
    {
        if (is_array($field)) {
            $data = $field;
        } else {
            $data[$field] = $value;
        }
        return $this->update($data);
    }

    /**
     * 字段值(延迟)增长
     * @access public
     * @param string $field 字段名
     * @param integer $step 增长值
     * @param integer $lazyTime 延时时间(s)
     * @return integer|true
     * @throws Exception
     */
    public function setInc($field, $step = 1, $lazyTime = 0)
    {
        $condition = !empty($this->options['where']) ? $this->options['where'] : [];
        if (empty($condition)) {
            // 没有条件不做任何更新
            throw new Exception('no data to update');
        }
        if ($lazyTime > 0) {
            // 延迟写入
            $guid = md5($this->getTable() . '_' . $field . '_' . serialize($condition));
            $step = $this->lazyWrite($guid, $step, $lazyTime);
            if (empty($step)) {
                return true; // 等待下次写入
            }
        }
        return $this->setField($field, ['$inc', $step]);
    }

    /**
     * 字段值（延迟）减少
     * @access public
     * @param string $field 字段名
     * @param integer $step 减少值
     * @param integer $lazyTime 延时时间(s)
     * @return integer|true
     * @throws Exception
     */
    public function setDec($field, $step = 1, $lazyTime = 0)
    {
        $condition = !empty($this->options['where']) ? $this->options['where'] : [];
        if (empty($condition)) {
            // 没有条件不做任何更新
            throw new Exception('no data to update');
        }
        if ($lazyTime > 0) {
            // 延迟写入
            $guid = md5($this->getTable() . '_' . $field . '_' . serialize($condition));
            $step = $this->lazyWrite($guid, -$step, $lazyTime);
            if (empty($step)) {
                return true; // 等待下次写入
            }
        }
        return $this->setField($field, ['$inc', -1 * $step]);
    }

    /**
     * 延时更新检查 返回false表示需要延时
     * 否则返回实际写入的数值
     * @access public
     * @param string $guid 写入标识
     * @param integer $step 写入步进值
     * @param integer $lazyTime 延时时间(s)
     * @return false|integer
     */
    protected function lazyWrite($guid, $step, $lazyTime)
    {
        if (false !== ($value = Cache::get($guid))) {
            // 存在缓存写入数据
            if ($_SERVER['REQUEST_TIME'] > Cache::get($guid . '_time') + $lazyTime) {
                // 延时更新时间到了，删除缓存数据 并实际写入数据库
                Cache::rm($guid);
                Cache::rm($guid . '_time');
                return $value + $step;
            } else {
                // 追加数据到缓存
                Cache::set($guid, $value + $step, 0);
                return false;
            }
        } else {
            // 没有缓存数据
            Cache::set($guid, $step, 0);
            // 计时开始
            Cache::set($guid . '_time', $_SERVER['REQUEST_TIME'], 0);
            return false;
        }
    }

    /**
     * 指定AND查询条件
     * @access public
     * @param mixed $field 查询字段
     * @param mixed $op 查询表达式
     * @param mixed $condition 查询条件
     * @return $this
     */
    public function where($field, $op = null, $condition = null)
    {
        $param = func_get_args();
        array_shift($param);
        $this->parseWhereExp('$and', $field, $op, $condition, $param);
        return $this;
    }

    /**
     * 指定OR查询条件
     * @access public
     * @param mixed $field 查询字段
     * @param mixed $op 查询表达式
     * @param mixed $condition 查询条件
     * @return $this
     */
    public function whereOr($field, $op = null, $condition = null)
    {
        $param = func_get_args();
        array_shift($param);
        $this->parseWhereExp('$or', $field, $op, $condition, $param);
        return $this;
    }

    /**
     * 指定NOR查询条件
     * @access public
     * @param mixed $field 查询字段
     * @param mixed $op 查询表达式
     * @param mixed $condition 查询条件
     * @return $this
     */
    public function whereNor($field, $op = null, $condition = null)
    {
        $param = func_get_args();
        array_shift($param);
        $this->parseWhereExp('$nor', $field, $op, $condition, $param);
        return $this;
    }

    /**
     * 分析查询表达式
     * @access public
     * @param string $logic 查询逻辑    and or xor
     * @param string|array|\Closure $field 查询字段
     * @param mixed $op 查询表达式
     * @param mixed $condition 查询条件
     * @param array $param 查询参数
     * @return void
     */
    protected function parseWhereExp($logic, $field, $op, $condition, $param = [])
    {
        if ($field instanceof \Closure) {
            $this->options['where'][$logic][] = is_string($op) ? [$op, $field] : $field;
            return;
        }
        $where = [];
        if (is_null($op) && is_null($condition)) {
            if (is_array($field)) {
                // 数组批量查询
                $where = $field;
            } elseif ($field) {
                // 字符串查询
                $where[] = ['exp', $field];
            } else {
                $where = '';
            }
        } elseif (is_array($op)) {
            $where[$field] = $param;
        } elseif (is_null($condition)) {
            // 字段相等查询
            $where[$field] = ['=', $op];
        } else {
            $where[$field] = [$op, $condition];
        }

        if (!empty($where)) {
            if (!isset($this->options['where'][$logic])) {
                $this->options['where'][$logic] = [];
            }
            $this->options['where'][$logic] = array_merge($this->options['where'][$logic], $where);
        }
    }

    /**
     * 查询日期或者时间
     * @access public
     * @param string $field 日期字段名
     * @param string $op 比较运算符或者表达式
     * @param string|array $range 比较范围
     * @return $this
     */
    public function whereTime($field, $op, $range = null)
    {
        if (is_null($range)) {
            // 使用日期表达式
            $date = getdate();
            switch (strtolower($op)) {
                case 'today':
                case 'd':
                    $range = 'today';
                    break;
                case 'week':
                case 'w':
                    $range = 'this week 00:00:00';
                    break;
                case 'month':
                case 'm':
                    $range = mktime(0, 0, 0, $date['mon'], 1, $date['year']);
                    break;
                case 'year':
                case 'y':
                    $range = mktime(0, 0, 0, 1, 1, $date['year']);
                    break;
                case 'yesterday':
                    $range = ['yesterday', 'today'];
                    break;
                case 'last week':
                    $range = ['last week 00:00:00', 'this week 00:00:00'];
                    break;
                case 'last month':
                    $range = [date('y-m-01', strtotime('-1 month')), mktime(0, 0, 0, $date['mon'], 1, $date['year'])];
                    break;
                case 'last year':
                    $range = [mktime(0, 0, 0, 1, 1, $date['year'] - 1), mktime(0, 0, 0, 1, 1, $date['year'])];
                    break;
            }
            $op = is_array($range) ? 'between' : '>';
        }
        $this->where($field, strtolower($op) . ' time', $range);
        return $this;
    }

    /**
     * 分页查询
     * @param int|null $listRows 每页数量
     * @param bool $simple 简洁模式
     * @param array $config 配置参数
     *                      page:当前页,
     *                      path:url路径,
     *                      query:url额外参数,
     *                      fragment:url锚点,
     *                      var_page:分页变量,
     *                      list_rows:每页数量
     *                      type:分页类名,
     *                      namespace:分页类命名空间
     * @return \think\paginator\Collection
     * @throws DbException
     */
    public function paginate($listRows = null, $simple = false, $config = [])
    {
        $config = array_merge(Config::get('paginate'), $config);
        $listRows = $listRows ?: $config['list_rows'];
        $class = strpos($config['type'], '\\') ? $config['type'] : '\\think\\paginator\\driver\\' . ucwords($config['type']);
        $page = isset($config['page']) ? (int)$config['page'] : call_user_func([
            $class,
            'getCurrentPage',
        ], $config['var_page']);

        $page = $page < 1 ? 1 : $page;

        $config['path'] = isset($config['path']) ? $config['path'] : call_user_func([$class, 'getCurrentPath']);

        /** @var Paginator $paginator */
        if (!$simple) {
            $options = $this->getOptions();
            $total = $this->count();
            $results = $this->options($options)->page($page, $listRows)->select();
        } else {
            $results = $this->limit(($page - 1) * $listRows, $listRows + 1)->select();
            $total = null;
        }
        return $class::make($results, $listRows, $page, $total, $simple, $config);
    }

    /**
     * 指定当前操作的数据表
     * @access public
     * @param string $table 表名
     * @return $this
     */
    public function table($table)
    {
        $this->options['table'] = $table;
        return $this;
    }

    /**
     * 查询缓存
     * @access public
     * @param mixed $key
     * @param integer $expire
     * @return $this
     */
    public function cache($key = true, $expire = null)
    {
        // 增加快捷调用方式 cache(10) 等同于 cache(true, 10)
        if (is_numeric($key) && is_null($expire)) {
            $expire = $key;
            $key = true;
        }
        if (false !== $key) {
            $this->options['cache'] = ['key' => $key, 'expire' => $expire];
        }
        return $this;
    }

    /**
     * 不主动获取数据集
     * @access public
     * @param bool $cursor 是否返回 Cursor 对象
     * @return $this
     */
    public function fetchCursor($cursor = true)
    {
        $this->options['fetch_class'] = $cursor;
        return $this;
    }

    /**
     * 指定数据集返回对象
     * @access public
     * @param string $class 指定返回的数据集对象类名
     * @return $this
     */
    public function fetchClass($class)
    {
        $this->options['fetch_class'] = $class;
        return $this;
    }

    /**
     * 设置typeMap
     * @access public
     * @param string|array $typeMap
     * @return $this
     */
    public function typeMap($typeMap)
    {
        $this->options['typeMap'] = $typeMap;
        return $this;
    }

    /**
     * 设置从主服务器读取数据
     * @access public
     * @return $this
     */
    public function master()
    {
        $this->options['master'] = true;
        return $this;
    }

    /**
     * 设置查询数据不存在是否抛出异常
     * @access public
     * @param bool $fail 是否严格检查字段
     * @return $this
     */
    public function failException($fail = true)
    {
        $this->options['fail'] = $fail;
        return $this;
    }

    /**
     * 设置查询数据不存在是否抛出异常
     * @access public
     * @param bool $awaitData
     * @return $this
     */
    public function awaitData($awaitData)
    {
        $this->options['awaitData'] = $awaitData;
        return $this;
    }

    /**
     * batchSize
     * @access public
     * @param integer $batchSize
     * @return $this
     */
    public function batchSize($batchSize)
    {
        $this->options['batchSize'] = $batchSize;
        return $this;
    }

    /**
     * exhaust
     * @access public
     * @param bool $exhaust
     * @return $this
     */
    public function exhaust($exhaust)
    {
        $this->options['exhaust'] = $exhaust;
        return $this;
    }

    /**
     * 设置modifiers
     * @access public
     * @param array $modifiers
     * @return $this
     */
    public function modifiers($modifiers)
    {
        $this->options['modifiers'] = $modifiers;
        return $this;
    }

    /**
     * 设置noCursorTimeout
     * @access public
     * @param bool $noCursorTimeout
     * @return $this
     */
    public function noCursorTimeout($noCursorTimeout)
    {
        $this->options['noCursorTimeout'] = $noCursorTimeout;
        return $this;
    }

    /**
     * 设置oplogReplay
     * @access public
     * @param bool $oplogReplay
     * @return $this
     */
    public function oplogReplay($oplogReplay)
    {
        $this->options['oplogReplay'] = $oplogReplay;
        return $this;
    }

    /**
     * 设置partial
     * @access public
     * @param bool $partial
     * @return $this
     */
    public function partial($partial)
    {
        $this->options['partial'] = $partial;
        return $this;
    }

    /**
     * 查询注释
     * @access public
     * @param string $comment 注释
     * @return $this
     */
    public function comment($comment)
    {
        $this->options['comment'] = $comment;
        return $this;
    }

    /**
     * maxTimeMS
     * @access public
     * @param string $maxTimeMS
     * @return $this
     */
    public function maxTimeMS($maxTimeMS)
    {
        $this->options['maxTimeMS'] = $maxTimeMS;
        return $this;
    }

    /**
     * 设置返回字段
     * @access public
     * @param array $field
     * @param boolean $except 是否排除
     * @return $this
     */
    public function field($field, $except = false)
    {
        if (is_string($field)) {
            $field = explode(',', $field);
        }
        $projection = [];
        foreach ($field as $key => $val) {
            if (is_numeric($key)) {
                $projection[$val] = $except ? 0 : 1;
            } else {
                $projection[$key] = $val;
            }
        }
        $this->options['projection'] = $projection;
        return $this;
    }

    /**
     * 设置skip
     * @access public
     * @param integer $skip
     * @return $this
     */
    public function skip($skip)
    {
        $this->options['skip'] = $skip;
        return $this;
    }

    /**
     * 设置slaveOk
     * @access public
     * @param bool $slaveOk
     * @return $this
     */
    public function slaveOk($slaveOk)
    {
        $this->options['slaveOk'] = $slaveOk;
        return $this;
    }

    /**
     * 关联预载入查询
     * @access public
     * @param mixed $with
     * @return $this
     */
    public function with($with)
    {
        return $this;
    }

    /**
     * 指定查询数量
     * @access public
     * @param mixed $offset 起始位置
     * @param mixed $length 查询数量
     * @return $this
     */
    public function limit($offset, $length = null)
    {
        if (is_null($length)) {
            if (is_numeric($offset)) {
                $length = $offset;
                $offset = 0;
            } else {
                list($offset, $length) = explode(',', $offset);
            }
        }
        $this->options['skip'] = intval($offset);
        $this->options['limit'] = intval($length);

        return $this;
    }

    /**
     * 指定分页
     * @access public
     * @param mixed $page 页数
     * @param mixed $listRows 每页数量
     * @return $this
     */
    public function page($page, $listRows = null)
    {
        if (is_null($listRows) && strpos($page, ',')) {
            list($page, $listRows) = explode(',', $page);
        }
        $this->options['page'] = [intval($page), intval($listRows)];
        return $this;
    }

    /**
     * 设置sort
     * @access public
     * @param array|string|object $field
     * @param string $order
     * @return $this
     */
    public function order($field, $order = '')
    {
        if (is_array($field)) {
            $this->options['sort'] = $field;
        } else {
            $this->options['sort'][$field] = 'asc' == strtolower($order) ? 1 : -1;
        }
        return $this;
    }

    /**
     * 设置tailable
     * @access public
     * @param bool $tailable
     * @return $this
     */
    public function tailable($tailable)
    {
        $this->options['tailable'] = $tailable;
        return $this;
    }

    /**
     * 设置writeConcern对象
     * @access public
     * @param WriteConcern $writeConcern
     * @return $this
     */
    public function writeConcern($writeConcern)
    {
        $this->options['writeConcern'] = $writeConcern;
        return $this;
    }

    /**
     * 获取当前数据表的主键
     * @access public
     * @return string|array
     */
    public function getPk()
    {
        return !empty($this->pk) ? $this->pk : $this->getConfig('pk');
    }

    /**
     * 查询参数赋值
     * @access protected
     * @param array $options 表达式参数
     * @return $this
     */
    public function options(array $options)
    {
        $this->options = $options;
        return $this;
    }

    /**
     * 获取当前的查询参数
     * @access public
     * @param string $name 参数名
     * @return mixed
     */
    public function getOptions($name = '')
    {
        return isset($this->options[$name]) ? $this->options[$name] : $this->options;
    }

    /**
     * 设置关联查询
     * @access public
     * @param string $relation 关联名称
     * @return $this
     */
    public function relation($relation)
    {
        $this->options['relation'] = $relation;
        return $this;
    }

    /**
     * 把主键值转换为查询条件 支持复合主键
     * @access public
     * @param array|string $data 主键数据
     * @param mixed $options 表达式参数
     * @return void
     * @throws Exception
     */
    protected function parsePkWhere($data, &$options)
    {
        $pk = $this->getPk();

        if (is_string($pk)) {
            // 根据主键查询
            if (is_array($data)) {
                $where[$pk] = isset($data[$pk]) ? $data[$pk] : ['in', $data];
            } else {
                $where[$pk] = strpos($data, ',') ? ['in', $data] : $data;
            }
        }

        if (!empty($where)) {
            if (isset($options['where']['$and'])) {
                $options['where']['$and'] = array_merge($options['where']['$and'], $where);
            } else {
                $options['where']['$and'] = $where;
            }
        }
        return;
    }

    /**
     * 插入记录
     * @access public
     * @param mixed $data 数据
     * @return WriteResult
     * @throws AuthenticationException
     * @throws InvalidArgumentException
     * @throws ConnectionException
     * @throws RuntimeException
     * @throws BulkWriteException
     */
    public function insert(array $data, $options = array(), $replace = false)
    {
        $this->table($options['table']);
        if (empty($data)) {
            throw new Exception('miss data to insert');
        }
        // 分析查询表达式
        $options = $this->parseExpress();
        // 生成bulk对象
        $bulk = $this->builder_insert($data, $options);
        $writeConcern = isset($options['writeConcern']) ? $options['writeConcern'] : null;
        $writeResult = $this->execute($options['table'], $bulk, $writeConcern);
        return $writeResult->getInsertedCount();
    }

    /**
     * 插入记录并获取自增ID
     * @access public
     * @param mixed $data 数据
     * @return integer
     * @throws AuthenticationException
     * @throws InvalidArgumentException
     * @throws ConnectionException
     * @throws RuntimeException
     * @throws BulkWriteException
     */
    public function insertGetId(array $data, $options = array(), $replace = false)
    {
        $this->table($options['table']);
        $this->insert($data);
        return $this->getLastInsID();
    }

    /**
     * 批量插入记录
     * @access public
     * @param mixed $dataSet 数据集
     * @return integer
     * @throws AuthenticationException
     * @throws InvalidArgumentException
     * @throws ConnectionException
     * @throws RuntimeException
     * @throws BulkWriteException
     */
    public function insertAll(array $dataSet,$options=array(),$replace=false)
    {
        $this->table($options['table']);
        // 分析查询表达式
        $options = $this->parseExpress();
        if (!is_array(reset($dataSet))) {
            return false;
        }

        // 生成bulkWrite对象
        $bulk = $this->builder_insertAll($dataSet, $options);
        $writeConcern = isset($options['writeConcern']) ? $options['writeConcern'] : null;
        $writeResult = $this->execute($options['table'], $bulk, $writeConcern);
        return $writeResult->getInsertedCount();
    }

    /**
     * 更新记录
     * @access public
     * @param mixed $data 数据
     * @return int
     * @throws Exception
     * @throws AuthenticationException
     * @throws InvalidArgumentException
     * @throws ConnectionException
     * @throws RuntimeException
     * @throws BulkWriteException
     */
    public function update(array $data, $options)
    {
        $this->table($options['table']);
        $options = $this->parseExpress();
        if (empty($options['where'])) {
            $pk = $this->getPk();
            // 如果存在主键数据 则自动作为更新条件
            if (is_string($pk) && isset($data[$pk])) {
                $where[$pk] = $data[$pk];
                $key = 'mongo:' . $options['table'] . '|' . $data[$pk];
                unset($data[$pk]);
            } elseif (is_array($pk)) {
                // 增加复合主键支持
                foreach ($pk as $field) {
                    if (isset($data[$field])) {
                        $where[$field] = $data[$field];
                    } else {
                        // 如果缺少复合主键数据则不执行
                        throw new Exception('miss complex primary data');
                    }
                    unset($data[$field]);
                }
            }
            if (!isset($where)) {
                // 如果没有任何更新条件则不执行
                throw new Exception('miss update condition');
            } else {
                $options['where']['$and'] = $where;
            }
        }

        // 生成bulkWrite对象
        $bulk = $this->builder->update($data, $options);
        $writeConcern = isset($options['writeConcern']) ? $options['writeConcern'] : null;
        $writeResult = $this->execute($options['table'], $bulk, $writeConcern);
        // 检测缓存
        if (isset($key) && Cache::get($key)) {
            // 删除缓存
            Cache::rm($key);
        }
        return $writeResult->getModifiedCount();
    }

    /**
     * 删除记录
     * @access public
     * @param array $data 表达式 true 表示强制删除
     * @return int
     * @throws Exception
     * @throws AuthenticationException
     * @throws InvalidArgumentException
     * @throws ConnectionException
     * @throws RuntimeException
     * @throws BulkWriteException
     */
    public function delete($data = null,$options = array())
    {
        $this->table($options['table']);
        // 分析查询表达式
        $options = $this->parseExpress();

        if (!is_null($data) && true !== $data) {
            if (!is_array($data)) {
                // 缓存标识
                $key = 'mongo:' . $options['table'] . '|' . $data;
            }
            // AR模式分析主键条件
            $this->parsePkWhere($data, $options);
        }

        if (true !== $data && empty($options['where'])) {
            // 如果不是强制删除且条件为空 不进行删除操作
            throw new Exception('delete without condition');
        }

        // 生成bulkWrite对象
        $bulk = $this->builder->delete($options);
        $writeConcern = isset($options['writeConcern']) ? $options['writeConcern'] : null;
        // 执行操作
        $writeResult = $this->execute($options['table'], $bulk, $writeConcern);
        // 检测缓存
        if (isset($key) && Cache::get($key)) {
            // 删除缓存
            Cache::rm($key);
        }
        return $writeResult->getDeletedCount();
    }

    /**
     * 查找记录
     * @access public
     * @param array|string|Query|\Closure $data
     * @return Collection|false|Cursor|string
     * @throws ModelNotFoundException
     * @throws DataNotFoundException
     * @throws AuthenticationException
     * @throws InvalidArgumentException
     * @throws ConnectionException
     * @throws RuntimeException
     */
    public function select($options = null, $data = null)
    {
        $this->table($options['table']);
        if (!empty($options)) $this->options = array_merge($this->options, $options);
        if ($data instanceof Query) {
            return $data->select();
        } elseif ($data instanceof \Closure) {
            call_user_func_array($data, [& $this]);
            $data = null;
        }
        // 分析查询表达式
        $options = $this->parseExpress();
        if (!is_null($data)) {
            // 主键条件分析
            $this->parsePkWhere($data, $options);
        }

        $resultSet = false;
        if (!empty($options['cache'])) {
            // 判断查询缓存
            $cache = $options['cache'];
            $key = is_string($cache['key']) ? $cache['key'] : md5(serialize($options));
            $resultSet = Cache::get($key);
        }
        if (!$resultSet) {
            // 生成MongoQuery对象
            //$query = $this->builder->select($options);
            $where = $this->parseWhere($options['where']);
            //$where['$and'] = ['uid'=>'9','title'=>'大清'];
            //var_dump($where);exit;
            //$where=[];
            //$where['title'] = '宋朝';/* */
            //var_dump($where);
            $query = new MongoQuery($where, $options);

            if ($this->getConfig('debug')) {
                //$this->log($type, $data, $options);
            }

            $this->log('find', $where, $options);
            //return $query;

            // 执行查询操作
            $readPreference = isset($options['readPreference']) ? $options['readPreference'] : null;
            $resultSet = $this->query($options['table'], $query, $readPreference, $options['fetch_class'], $options['typeMap']);
            if ($resultSet instanceof Cursor) {
                // 返回MongoDB\Driver\Cursor对象
                return $resultSet;
            }

            if (isset($cache)) {
                // 缓存数据集
                Cache::set($key, $resultSet, $cache['expire']);
            }
        }

        // 返回结果处理
        if ($resultSet) {
            // 数据列表读取后的处理
            if (!empty($this->model_111111111111)) { //由于tp3.2有设置model属性，导致执行了以下代码，所以改名，令其不执行
                // 生成模型对象
                $model = $this->model;
                foreach ($resultSet as $key => $result) {
                    /** @var Model $result */
                    $result = new $model($result);
                    $result->isUpdate(true);
                    // 关联查询
                    if (!empty($options['relation'])) {
                        $result->relationQuery($options['relation']);
                    }
                    $resultSet[$key] = $result;
                }
                if (!empty($options['with'])) {
                    // 预载入
                    $resultSet = $result->eagerlyResultSet($resultSet, $options['with'], is_object($resultSet) ? get_class($resultSet) : '');
                }
            }
        } elseif (!empty($options['fail'])) {
            $this->throwNotFound($options);
        }
        return $resultSet;
    }

    /**
     * 查找单条记录
     * @access public
     * @param array|string|Query|\Closure $data
     * @return array|false|Cursor|string|Model
     * @throws ModelNotFoundException
     * @throws DataNotFoundException
     * @throws AuthenticationException
     * @throws InvalidArgumentException
     * @throws ConnectionException
     * @throws RuntimeException
     */
    public function find($data = null)
    {
        if ($data instanceof Query) {
            return $data->find();
        } elseif ($data instanceof \Closure) {
            call_user_func_array($data, [& $this]);
            $data = null;
        }
        // 分析查询表达式
        $options = $this->parseExpress();
        var_dump($options);
        exit('x');
        if (!is_null($data)) {
            // AR模式分析主键条件
            $this->parsePkWhere($data, $options);
        }

        $options['limit'] = 1;
        $result = false;
        if (!empty($options['cache'])) {
            // 判断查询缓存
            $cache = $options['cache'];
            if (true === $cache['key'] && !is_null($data) && !is_array($data)) {
                $key = 'mongo:' . $options['table'] . '|' . $data;
            } else {
                $key = is_string($cache['key']) ? $cache['key'] : md5(serialize($options));
            }
            $result = Cache::get($key);
        }
        if (!$result) {
            // 生成查询SQL
            $query = $this->builder->select($options);
            // 执行查询
            $readPreference = isset($options['readPreference']) ? $options['readPreference'] : null;
            $result = $this->query($options['table'], $query, $readPreference, $options['fetch_class'], $options['typeMap']);

            if ($result instanceof Cursor) {
                // 返回MongoDB\Driver\Cursor对象
                return $result;
            }

            if (isset($cache)) {
                // 缓存数据
                Cache::set($key, $result, $cache['expire']);
            }
        }

        // 数据处理
        if (!empty($result[0])) {
            $data = $result[0];
            if (!empty($this->model)) {
                // 返回模型对象
                $model = $this->model;
                $data = new $model($data);
                $data->isUpdate(true, isset($options['where']['$and']) ? $options['where']['$and'] : null);
                // 关联查询
                if (!empty($options['relation'])) {
                    $data->relationQuery($options['relation']);
                }
                if (!empty($options['with'])) {
                    // 预载入
                    $data->eagerlyResult($data, $options['with'], is_object($result) ? get_class($result) : '');
                }
            }
        } elseif (!empty($options['fail'])) {
            $this->throwNotFound($options);
        } else {
            $data = null;
        }
        return $data;
    }

    /**
     * 查询失败 抛出异常
     * @access public
     * @param array $options 查询参数
     * @throws ModelNotFoundException
     * @throws DataNotFoundException
     */
    protected function throwNotFound($options = [])
    {
        if (!empty($this->model)) {
            throw new ModelNotFoundException('model data Not Found:' . $this->model, $this->model, $options);
        } else {
            throw new DataNotFoundException('table data not Found:' . $options['table'], $options['table'], $options);
        }
    }

    /**
     * 查找多条记录 如果不存在则抛出异常
     * @access public
     * @param array|string|Query|\Closure $data
     * @return array|\PDOStatement|string|Model
     * @throws ModelNotFoundException
     * @throws DataNotFoundException
     * @throws AuthenticationException
     * @throws InvalidArgumentException
     * @throws ConnectionException
     * @throws RuntimeException
     */
    public function selectOrFail($data = null)
    {
        return $this->failException(true)->select($data);
    }

    /**
     * 查找单条记录 如果不存在则抛出异常
     * @access public
     * @param array|string|Query|\Closure $data
     * @return array|\PDOStatement|string|Model
     * @throws ModelNotFoundException
     * @throws DataNotFoundException
     * @throws AuthenticationException
     * @throws InvalidArgumentException
     * @throws ConnectionException
     * @throws RuntimeException
     */
    public function findOrFail($data = null)
    {
        return $this->failException(true)->find($data);
    }

    /**
     * 分批数据返回处理
     * @access public
     * @param integer $count 每次处理的数据数量
     * @param callable $callback 处理回调方法
     * @param string $column 分批处理的字段名
     * @return boolean
     */
    public function chunk($count, $callback, $column = null)
    {
        $column = $column ?: $this->getPk();
        $options = $this->getOptions();
        $resultSet = $this->limit($count)->order($column, 'asc')->select();

        while (!empty($resultSet)) {
            if (false === call_user_func($callback, $resultSet)) {
                return false;
            }
            $end = end($resultSet);
            $lastId = is_array($end) ? $end[$column] : $end->$column;
            $resultSet = $this->options($options)
                ->limit($count)
                ->where($column, '>', $lastId)
                ->order($column, 'asc')
                ->select();
        }
        return true;
    }

    /**
     * 获取数据表信息
     * @access public
     * @param string $tableName 数据表名 留空自动获取
     * @param string $fetch 获取信息类型 包括 fields type pk
     * @return mixed
     */
    public function getTableInfo($tableName = '', $fetch = '')
    {
        if (!$tableName) {
            $tableName = $this->getTable();
        }
        if (is_array($tableName)) {
            $tableName = key($tableName) ?: current($tableName);
        }

        if (strpos($tableName, ',')) {
            // 多表不获取字段信息
            return false;
        } else {
            $tableName = $this->parseSqlTable($tableName);
        }

        $guid = md5($tableName);
        if (!isset(self::$info[$guid])) {
            $result = $this->table($tableName)->find();
            $fields = array_keys($result);
            $type = [];
            foreach ($result as $key => $val) {
                // 记录字段类型
                $type[$key] = getType($val);
                if ('_id' == $key) {
                    $pk = $key;
                }
            }
            if (!isset($pk)) {
                // 设置主键
                $pk = null;
            }
            $result = ['fields' => $fields, 'type' => $type, 'pk' => $pk];
            self::$info[$guid] = $result;
        }
        return $fetch ? self::$info[$guid][$fetch] : self::$info[$guid];
    }

    /**
     * 分析表达式（可用于查询或者写入操作）
     * @access protected
     * @return array
     */
    protected function parseExpress()
    {
        $options = $this->options;

        // 获取数据表
        if (empty($options['table'])) {
            $options['table'] = $this->getTable();
        }

        if (!isset($options['where'])) {
            $options['where'] = [];
        }

        $modifiers = empty($options['modifiers']) ? [] : $options['modifiers'];
        if (isset($options['comment'])) {
            $modifiers['$comment'] = $options['comment'];
        }

        if (isset($options['maxTimeMS'])) {
            $modifiers['$maxTimeMS'] = $options['maxTimeMS'];
        }

        if (!empty($modifiers)) {
            $options['modifiers'] = $modifiers;
        }

        if (!isset($options['projection']) || '*' == $options['projection']) {
            $options['projection'] = [];
        }

        if (!isset($options['typeMap'])) {
            $options['typeMap'] = $this->getConfig('type_map');
        }

        if (!isset($options['limit'])) {
            $options['limit'] = 0;
        }

        foreach (['master', 'fetch_class'] as $name) {
            if (!isset($options[$name])) {
                $options[$name] = false;
            }
        }

        if (isset($options['page'])) {
            // 根据页数计算limit
            list($page, $listRows) = $options['page'];
            $page = $page > 0 ? $page : 1;
            $listRows = $listRows > 0 ? $listRows : (is_numeric($options['limit']) ? $options['limit'] : 20);
            $offset = $listRows * ($page - 1);
            $options['skip'] = intval($offset);
            $options['limit'] = intval($listRows);
        }
        $this->options = [];
        return $options;
    }






























    //===========================================builder===========================================================
    /**
     * key分析
     * @access protected
     * @param string $key
     * @return string
     */
    protected function parseKey($key)
    {
        if ('id' == $key && $this->connection->getConfig('pk_convert_id')) {
            $key = '_id';
        }
        return trim($key);
    }

    /**
     * value分析
     * @access protected
     * @param mixed $value
     * @param string $field
     * @return string
     */
    protected function parseValue($value, $field = '')
    {
        if ('_id' == $field && !($value instanceof ObjectID)) {
            return new ObjectID($value);
        }
        return $value;
    }

    /**
     * insert数据分析
     * @access protected
     * @param array $data 数据
     * @param array $options 查询参数
     * @return array
     */
    protected function parseData($data, $options)
    {
        if (empty($data)) {
            return [];
        }

        $result = [];
        foreach ($data as $key => $val) {
            $item = $this->parseKey($key);
            if (is_object($val)) {
                $result[$item] = $val;
            } elseif (isset($val[0]) && 'exp' == $val[0]) {
                $result[$item] = $val[1];
            } elseif (is_null($val)) {
                $result[$item] = 'NULL';
            } else {
                $result[$item] = $this->parseValue($val, $key);
            }
        }
        return $result;
    }

    /**
     * Set数据分析
     * @access protected
     * @param array $data 数据
     * @param array $options 查询参数
     * @return array
     */
    protected function parseSet($data, $options)
    {
        if (empty($data)) {
            return [];
        }

        $result = [];
        foreach ($data as $key => $val) {
            $item = $this->parseKey($key);
            if (is_array($val) && isset($val[0]) && in_array($val[0], ['$inc', '$set', '$unset', '$push', '$pushall', '$addtoset', '$pop', '$pull', '$pullall'])) {
                $result[$val[0]][$item] = $this->parseValue($val[1], $key);
            } else {
                $result['$set'][$item] = $this->parseValue($val, $key);
            }
        }
        return $result;
    }

    /**
     * 生成查询过滤条件
     * @access public
     * @param mixed $where
     * @return array
     */
    public function parseWhere($where)
    {
        if (empty($where)) {
            $where = [];
        }

        $filter = [];
        foreach ($where as $logic => $val) {
            foreach ($val as $field => $value) {
                if ($value instanceof \Closure) {
                    // 使用闭包查询
                    $query = new Query($this->connection);
                    call_user_func_array($value, [& $query]);
                    $filter[$logic][] = $this->parseWhere($query->getOptions('where')[$logic]);
                } else {
                    if (strpos($field, '|')) {
                        // 不同字段使用相同查询条件（OR）
                        $array = explode('|', $field);
                        foreach ($array as $k) {
                            $filter['$or'][] = $this->parseWhereItem($k, $value);
                        }
                    } elseif (strpos($field, '&')) {
                        // 不同字段使用相同查询条件（AND）
                        $array = explode('&', $field);
                        foreach ($array as $k) {
                            $filter['$and'][] = $this->parseWhereItem($k, $value);
                        }
                    } else {
                        // 对字段使用表达式查询
                        $field = is_string($field) ? $field : '';
                        $filter[$logic][] = $this->parseWhereItem($field, $value);
                    }
                }
            }
        }
        return $filter;
    }

    // where子单元分析
    protected function parseWhereItem($field, $val)
    {
        $key = $field ? $this->parseKey($field) : '';
        // 查询规则和条件
        if (!is_array($val)) {
            $val = ['=', $val];
        }
        list($exp, $value) = $val;

        // 对一个字段使用多个查询条件
        if (is_array($exp)) {
            $data = [];
            foreach ($val as $value) {
                $exp = $value[0];
                $value = $value[1];
                if (!in_array($exp, $this->exp)) {
                    $exp = strtolower($exp);
                    if (isset($this->exp[$exp])) {
                        $exp = $this->exp[$exp];
                    }
                }
                $k = '$' . $exp;
                $data[$k] = $value;
            }
            $query[$key] = $data;
            return $query;
        } elseif (!in_array($exp, $this->exp)) {
            $exp = strtolower($exp);
            if (isset($this->exp[$exp])) {
                $exp = $this->exp[$exp];
            } else {
                throw new Exception('where express error:' . $exp);
            }
        }

        $query = [];
        if ('=' == $exp) {
            // 普通查询
            $query[$key] = $this->parseValue($value, $key);
        } elseif (in_array($exp, ['neq', 'ne', 'gt', 'egt', 'gte', 'lt', 'lte', 'elt', 'mod'])) {
            // 比较运算
            $k = '$' . $exp;
            $query[$key] = [$k => $this->parseValue($value, $key)];
        } elseif ('all' == $exp) {
            // 满足所有指定条件
            $query[$key] = ['$all', $this->parseValue($value, $key)];
        } elseif ('between' == $exp) {
            // 区间查询
            $value = is_array($value) ? $value : explode(',', $value);
            $query[$key] = ['$gte' => $this->parseValue($value[0], $key), '$lte' => $this->parseValue($value[1], $key)];
        } elseif ('not between' == $exp) {
            // 范围查询
            $value = is_array($value) ? $value : explode(',', $value);
            $query[$key] = ['$lt' => $this->parseValue($value[0], $key), '$gt' => $this->parseValue($value[1], $key)];
        } elseif ('exists' == $exp) {
            // 字段是否存在
            $query[$key] = ['$exists' => (bool)$value];
        } elseif ('type' == $exp) {
            // 类型查询
            $query[$key] = ['$type' => intval($value)];
        } elseif ('exp' == $exp) {
            // 表达式查询
            $query['$where'] = $value instanceof Javascript ? $value : new Javascript($value);
        } elseif ('like' == $exp) {
            // 模糊查询 采用正则方式
            $query[$key] = $value instanceof Regex ? $value : new Regex($value, 'i');
        } elseif (in_array($exp, ['nin', 'in'])) {
            // IN 查询
            $value = is_array($value) ? $value : explode(',', $value);
            foreach ($value as $k => $val) {
                $value[$k] = $this->parseValue($val, $key);
            }
            $query[$key] = ['$' . $exp => $value];
        } elseif ('regex' == $exp) {
            $query[$key] = $value instanceof Regex ? $value : new Regex($value, 'i');
        } elseif ('< time' == $exp) {
            $query[$key] = ['$lt' => $this->parseDateTime($value, $field)];
        } elseif ('> time' == $exp) {
            $query[$key] = ['$gt' => $this->parseDateTime($value, $field)];
        } elseif ('between time' == $exp) {
            // 区间查询
            $value = is_array($value) ? $value : explode(',', $value);
            $query[$key] = ['$gte' => $this->parseDateTime($value[0], $field), '$lte' => $this->parseDateTime($value[1], $field)];
        } elseif ('not between time' == $exp) {
            // 范围查询
            $value = is_array($value) ? $value : explode(',', $value);
            $query[$key] = ['$lt' => $this->parseDateTime($value[0], $field), '$gt' => $this->parseDateTime($value[1], $field)];
        } elseif ('near' == $exp) {
            // 经纬度查询
            $query[$key] = ['$near' => $this->parseValue($value, $key)];
        } else {
            // 普通查询
            $query[$key] = $this->parseValue($value, $key);
        }
        return $query;
    }

    /**
     * 日期时间条件解析
     * @access protected
     * @param string $value
     * @param string $key
     * @return string
     */
    protected function parseDateTime($value, $key)
    {
        // 获取时间字段类型
        $type = $this->query->getTableInfo('', 'type');
        if (isset($type[$key])) {
            $value = strtotime($value) ?: $value;
            if (preg_match('/(datetime|timestamp)/is', $type[$key])) {
                // 日期及时间戳类型
                $value = date('Y-m-d H:i:s', $value);
            } elseif (preg_match('/(date)/is', $type[$key])) {
                // 日期及时间戳类型
                $value = date('Y-m-d', $value);
            }
        }
        return $value;
    }

    /**
     * 获取最后写入的ID 如果是insertAll方法的话 返回所有写入的ID
     * @access public
     * @return mixed
     */
    public function builder_getLastInsID()
    {
        return $this->insertId;
    }

    /**
     * 生成insert BulkWrite对象
     * @access public
     * @param array $data 数据
     * @param array $options 表达式
     * @return BulkWrite
     */
    public function builder_insert(array $data, $options = [])
    {
        // 分析并处理数据
        $data = $this->parseData($data, $options);
        $bulk = new BulkWrite;
        if ($insertId = $bulk->insert($data)) {
            $this->insertId = $insertId;
        }
        $this->log('insert', $data, $options);
        return $bulk;
    }

    /**
     * 生成insertall BulkWrite对象
     * @access public
     * @param array $dataSet 数据集
     * @param array $options 参数
     * @return BulkWrite
     */
    public function builder_insertAll($dataSet, $options = [])
    {
        $bulk = new BulkWrite;
        foreach ($dataSet as $data) {
            // 分析并处理数据
            $data = $this->parseData($data, $options);
            if ($insertId = $bulk->insert($data)) {
                $this->insertId[] = $insertId;
            }
        }
        $this->log('insert', $dataSet, $options);
        return $bulk;
    }

    /**
     * 生成update BulkWrite对象
     * @access public
     * @param array $data 数据
     * @param array $options 参数
     * @return BulkWrite
     */
    public function builder_update($data, $options = [])
    {
        $data = $this->parseSet($data, $options);
        $where = $this->parseWhere($options['where']);

        if (1 == $options['limit']) {
            $updateOptions = ['multi' => false];
        } else {
            $updateOptions = ['multi' => true];
        }
        $bulk = new BulkWrite;
        $bulk->update($where, $data, $updateOptions);
        $this->log('update', $data, $where);
        return $bulk;
    }

    /**
     * 生成delete BulkWrite对象
     * @access public
     * @param array $options 参数
     * @return BulkWrite
     */
    public function builder_delete($options)
    {
        $where = $this->parseWhere($options['where']);
        $bulk = new BulkWrite;
        if (1 == $options['limit']) {
            $deleteOptions = ['limit' => 1];
        } else {
            $deleteOptions = ['limit' => 0];
        }
        $bulk->delete($where, $deleteOptions);
        $this->log('remove', $where, $deleteOptions);
        return $bulk;
    }

    /**
     * 生成Mongo查询对象
     * @access public
     * @param array $options 参数
     * @return MongoQuery
     */
    public function builder_select($options)
    {
        $where = $this->parseWhere($options['where']);
        $query = new MongoQuery($where, $options);
        $this->log('find', $where, $options);
        return $query;
    }

    /**
     * 生成Count命令
     * @access public
     * @param array $options 参数
     * @return Command
     */
    public function builder_count($options)
    {
        $cmd['count'] = $options['table'];
        $cmd['query'] = $this->parseWhere($options['where']);
        foreach (['hint', 'limit', 'maxTimeMS', 'skip'] as $option) {
            if (isset($options[$option])) {
                $cmd[$option] = $options[$option];
            }
        }
        $command = new Command($cmd);
        $this->log('cmd', 'count', $cmd);
        return $command;
    }

    /**
     * 生成distinct命令
     * @access public
     * @param array $options 参数
     * @param string $field 字段名
     * @return Command
     */
    public function builder_distinct($options, $field)
    {
        $cmd = [
            'distinct' => $options['table'],
            'key' => $field,
        ];

        if (!empty($options['where'])) {
            $cmd['query'] = $this->parseWhere($options['where']);
        }

        if (isset($options['maxTimeMS'])) {
            $cmd['maxTimeMS'] = $options['maxTimeMS'];
        }
        $command = new Command($cmd);
        $this->log('cmd', 'distinct', $cmd);
        return $command;
    }

    /**
     * 查询所有的collection
     * @access public
     * @return Command
     */
    public function builder_listcollections()
    {
        $cmd = ['listCollections' => 1];
        $command = new Command($cmd);
        $this->log('cmd', 'listCollections', $cmd);
        return $command;
    }

    /**
     * 查询数据表的状态信息
     * @access public
     * @return Command
     */
    public function collStats($options)
    {
        $cmd = ['collStats' => $options['table']];
        $command = new Command($cmd);
        $this->log('cmd', 'collStats', $cmd);
        return $command;
    }

    protected function log2($type, $data, $options = [])
    {
        if ($this->connection->getConfig('debug')) {
            $this->log($type, $data, $options);
        }
    }


}