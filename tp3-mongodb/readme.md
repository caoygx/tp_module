人生悲剧啊！tp3的where(),limit()方法是在model里实现的，但mongodb 驱动没有直接继承model,所以where,limit无法重写model里的。那只能修改model了，有点丑陋。

model.class.php 1797行改为:
  	public function where($where,$parse=null){
        if($this->db instanceof \Think\Db\Driver\Mongo){
            $this->db->where($where,  null,  null);
            return $this;
        }

        if(!is_null($parse) && is_string($where)) {
        ......
    }


 model.class.php 1834行改为:
  	public function limit($offset,$length=null){
        if($this->db instanceof \Think\Db\Driver\Mongo){
            $this->db->limit($offset, $length );
            return $this;
        }
        if(is_null($length) && strpos($offset,',')){
        ......
    }