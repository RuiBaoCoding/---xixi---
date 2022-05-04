#include "page/bitmap_page.h"

/*
Bitmap Page Meta包括已经分配的页的数量以及下一个空闲的数据页
Bitmap Content存
bytes[]存某一页是否有空余
bytes中元素为char类型每一个char是两个十六进制数字，能够表示8个数据页的分布情况
因此表示的信息也是size*8
page_offset就是存的第几个文件
bit_offset是在这个byte中的第几个bit
byte_offset是第几个byte
*/

template<size_t PageSize>
//分配一个空闲页并通过page_offset返回所分配的空闲页位于该段中的下标
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if(next_free_page_>=MAX_CHARS*8){//存满
    return false;
  }else{
    page_allocated_++;//总数目增加1
    page_offset=next_free_page_;
    uint32_t byte_index=next_free_page_/8;
    uint8_t bit_index=next_free_page_%8;
    unsigned char a=bytes[byte_index];
    int ob[8];//二进制数组
    //int i=7;
    for(int i=7;i>=0;i--){
        ob[i]=a%2;
        //cout<<ob[i]<<endl;
        a/=2;
    }
    ob[bit_index]=1;
    //更新bytes的值
    bytes[byte_index]=0;
    for(int i=0;i<8;i++){
        bytes[byte_index]=bytes[byte_index]*2+ob[i];
    }
    //更新next_page_free_的值
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
//回收已经被分配的页
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  //首先把该页对应的数字置零,如果原来已经是0则返回false
  uint32_t byte_index=page_offset/8;
  uint8_t bit_index=page_offset%8;
  unsigned char a=bytes[byte_index];
  int ob[8];//二进制数组
  //int i=7;
  for(int i=7;i>=0;i--){
      ob[i]=a%2;
      //cout<<ob[i]<<endl;
      a/=2;
  }
  if(ob[bit_index]==0){//已经为空闲
    return false;
  }else{//可以被释放
    ob[bit_index]=0;
    page_allocated_--;//总数目减少1
    //转化为10进制后存回去
    bytes[byte_index]=0;
    for(int i=0;i<8;i++){
        bytes[byte_index]=bytes[byte_index]*2+ob[i];
    }
    if(page_offset<next_free_page_){
      //在下一个空闲页之前->更新下一个空闲页
      next_free_page_=page_offset;
    }
    return true;
  }
}

template<size_t PageSize>
//判断给定的page_offset是否未分配
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  //由于bytes中的每一个元素代表的是8个页的分配空间，所以需要对输入做一些处理
  //0表示空闲，1表示已分配
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