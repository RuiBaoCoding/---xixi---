#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) :capacity(num_pages) {
  //��ʼ���������
}

LRUReplacer::~LRUReplacer() = default;

//lru_list_���ܹ����滻������

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  //����������ٱ����ʵ�ҳ��ҳ֡�ţ�����true
  //��ǰû�п����滻�ķ���false
  if(lru_list_.size()==0){
    return false; //���˷���false
  }else{
    *frame_id=lru_list_.back();
    //������ά������������У����ľ���������ٷ��ʵ�
    lru_hash.erase(*frame_id);//��ӳ���ϵ��ɾ��

    lru_list_.pop_back();//����ɾ��
    return true;
  }
}

//��������Ѿ������ˣ���ôPin��Unpin������������

void LRUReplacer::Pin(frame_id_t frame_id) {
  if(lru_hash.count(frame_id)){//������
    auto p=lru_hash[frame_id];//ȡ��������
    lru_hash.erase(frame_id);//ɾ��ӳ���ϵ
    lru_list_.erase(p);//���б���ɾ��
  }
  //�������Ѿ���������ʲôҲ����
  //��������lru_list_���Ƴ���

}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  //���������ŵ�lru_list_��
  if(lru_hash.count(frame_id)){//�Ѿ�����
    return;
  }else if(lru_list_.size()==capacity){//�Ѿ�����
    return;
  }else{
    lru_list_.push_front(frame_id);//��ӵ�ͷ,��Ϊ�������ʹ��
    auto p=lru_list_.begin();
    lru_hash.emplace(frame_id,p);
  }
}

size_t LRUReplacer::Size() {
  //�����ܹ����滻������ҳ������
  return lru_list_.size();
  //return 0;
}