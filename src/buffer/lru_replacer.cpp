#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) {

}

LRUReplacer::~LRUReplacer() = default;

//lru_list_是能够被替换的序列

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  //返回最近最少被访问的页的页帧号，返回true
  //当前没有可以替换的返回false
  if(lru_times.size()==0){
    return false;
  }else{
    auto min=lru_times.begin();
    for(auto p=lru_times.begin();p!=lru_times.end();p++){
      if(p->second<min->second){
        min=p;
      }
    }
    *frame_id=min->first;
    lru_times.erase(min->first);
    return true;
  }
}

//如果数据已经不在了，那么Pin和Unpin都不起作用了

void LRUReplacer::Pin(frame_id_t frame_id) {
  lru_times.erase(frame_id);
  if(pin_times.count(frame_id)){
    pin_times[frame_id]++;
  }else{
    pin_times[frame_id]=1;
  }
  //上锁，从lru_list_中移出来

}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  //解锁，并放到lru_list_中
  if(pin_times.count(frame_id)){//已经存在
    lru_times[frame_id]=pin_times[frame_id];
  }else{
    lru_times[frame_id]=0;
  }
}

size_t LRUReplacer::Size() {
  //返回能够被替换的数据页的数量
  return lru_times.size();
  //return 0;
}