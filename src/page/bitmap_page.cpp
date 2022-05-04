#include "page/bitmap_page.h"

/*
Bitmap Page Meta�����Ѿ������ҳ�������Լ���һ�����е�����ҳ
Bitmap Content��
bytes[]��ĳһҳ�Ƿ��п���
bytes��Ԫ��Ϊchar����ÿһ��char������ʮ���������֣��ܹ���ʾ8������ҳ�ķֲ����
��˱�ʾ����ϢҲ��size*8
page_offset���Ǵ�ĵڼ����ļ�
bit_offset�������byte�еĵڼ���bit
byte_offset�ǵڼ���byte
*/

template<size_t PageSize>
//����һ������ҳ��ͨ��page_offset����������Ŀ���ҳλ�ڸö��е��±�
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if(next_free_page_>=MAX_CHARS*8){//����
    return false;
  }else{
    page_allocated_++;//����Ŀ����1
    page_offset=next_free_page_;
    uint32_t byte_index=next_free_page_/8;
    uint8_t bit_index=next_free_page_%8;
    unsigned char a=bytes[byte_index];
    int ob[8];//����������
    //int i=7;
    for(int i=7;i>=0;i--){
        ob[i]=a%2;
        //cout<<ob[i]<<endl;
        a/=2;
    }
    ob[bit_index]=1;
    //����bytes��ֵ
    bytes[byte_index]=0;
    for(int i=0;i<8;i++){
        bytes[byte_index]=bytes[byte_index]*2+ob[i];
    }
    //����next_page_free_��ֵ
    uint32_t i=next_free_page_;
    for(;i<MAX_CHARS*8;i++){
      if(IsPageFree(i)==true){
        break;
      }
    }
    next_free_page_=i;

    return true;
  }
}

template<size_t PageSize>
//�����Ѿ��������ҳ
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  //���ȰѸ�ҳ��Ӧ����������,���ԭ���Ѿ���0�򷵻�false
  uint32_t byte_index=page_offset/8;
  uint8_t bit_index=page_offset%8;
  unsigned char a=bytes[byte_index];
  int ob[8];//����������
  //int i=7;
  for(int i=7;i>=0;i--){
      ob[i]=a%2;
      //cout<<ob[i]<<endl;
      a/=2;
  }
  if(ob[bit_index]==0){//�Ѿ�Ϊ����
    return false;
  }else{//���Ա��ͷ�
    ob[bit_index]=0;
    page_allocated_--;//����Ŀ����1
    //ת��Ϊ10���ƺ���ȥ
    bytes[byte_index]=0;
    for(int i=0;i<8;i++){
        bytes[byte_index]=bytes[byte_index]*2+ob[i];
    }
    if(page_offset<next_free_page_){
      //����һ������ҳ֮ǰ->������һ������ҳ
      next_free_page_=page_offset;
    }
    return true;
  }
}

template<size_t PageSize>
//�жϸ�����page_offset�Ƿ�δ����
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  //����bytes�е�ÿһ��Ԫ�ش������8��ҳ�ķ���ռ䣬������Ҫ��������һЩ����
  //0��ʾ���У�1��ʾ�ѷ���
  uint32_t byte_index=page_offset/8;
  uint8_t bit_index=page_offset%8;
  unsigned char a=bytes[byte_index];
  int ob[8];
  //int i=7;
  for(int i=7;i>=0;i--){
      ob[i]=a%2;
      //cout<<ob[i]<<endl;
      a/=2;
  }
  if(ob[bit_index]==0){
    return true;
  }
  return false;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return false;
}

template
class BitmapPage<64>;

template
class BitmapPage<128>;

template
class BitmapPage<256>;

template
class BitmapPage<512>;

template
class BitmapPage<1024>;

template
class BitmapPage<2048>;

template
class BitmapPage<4096>;