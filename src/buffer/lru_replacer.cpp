#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) :capacity(num_pages) {
  //初始化最大容量
}

LRUReplacer::~LRUReplacer() = default;

//lru_list_是能够被替换的序列

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  //返回最近最少被访问的页的页帧号，返回true
  //当前没有可以替换的返回false
  if(lru_list_.size()==0){
    return false; //空了返回false
  }else{
    *frame_id=lru_list_.back();
    //在我们维护的这个链表中，最后的就是最近最少访问的
    lru_hash.erase(*frame_id);//从映射关系中删除

    lru_list_.pop_back();//将其删除
    return true;
  }
}

//如果数据已经不在了，那么Pin和Unpin都不起作用了

void LRUReplacer::Pin(frame_id_t frame_id) {
  if(lru_hash.count(frame_id)){//在里面
    auto p=lru_hash[frame_id];//取出迭代器
    lru_hash.erase(frame_id);//删除映射关系
    lru_list_.erase(p);//从列表中删除
  }
  //若数据已经不存在则什么也不做
  //上锁，从lru_list_中移出来

}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  //解锁，并放到lru_list_中
  if(lru_hash.count(frame_id)){//已经存在
    return;
  }else if(lru_list_.size()==capacity){//已经满了
    return;
  }else{
    lru_list_.push_front(frame_id);//添加到头,因为是最近被使用
    auto p=lru_list_.begin();
    lru_hash.emplace(frame_id,p);
  }
}

size_t LRUReplacer::Size() {
  //返回能够被替换的数据页的数量
  return lru_list_.size();
  //return 0;
}