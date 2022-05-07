#include "record/row.h"

uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  // replace with your code here
  
  char *buffer = buf;
  int len = fields_.size();
  for(int i=0;i<len;i++){
    auto tmp = this->fields_[i];//取出row中的每个Field指针
    buffer+=tmp->SerializeTo(buffer);
  }
  return buffer-buf;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  // replace with your code here
  auto len=schema->GetColumnCount();
  Field *df = nullptr;
  MemHeap *heap = new SimpleMemHeap();
  char *buffer = buf;
  for(auto i=0;i<len;i++){ 
    buffer+=fields_[i]->DeserializeFrom(buffer, schema->GetColumn(i)->GetType(), &fields_[i], false, heap);
  }
  return buffer-buf;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  // replace with your code here
  return 0;
}
