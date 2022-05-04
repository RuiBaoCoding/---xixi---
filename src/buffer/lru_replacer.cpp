#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) {

}

LRUReplacer::~LRUReplacer() = default;

//lru_list_���ܹ����滻������

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  //����������ٱ����ʵ�ҳ��ҳ֡�ţ�����true
  //��ǰû�п����滻�ķ���false
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

//��������Ѿ������ˣ���ôPin��Unpin������������

void LRUReplacer::Pin(frame_id_t frame_id) {
  lru_times.erase(frame_id);
  if(pin_times.count(frame_id)){
    pin_times[frame_id]++;
  }else{
    pin_times[frame_id]=1;
  }
  //��������lru_list_���Ƴ���

}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  //���������ŵ�lru_list_��
  if(pin_times.count(frame_id)){//�Ѿ�����
    lru_times[frame_id]=pin_times[frame_id];
  }else{
    lru_times[frame_id]=0;
  }
}

size_t LRUReplacer::Size() {
  //�����ܹ����滻������ҳ������
  return lru_times.size();
  //return 0;
}