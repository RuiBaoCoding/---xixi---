#ifndef MINISQL_B_PLUS_TREE_INTERNAL_PAGE_H
#define MINISQL_B_PLUS_TREE_INTERNAL_PAGE_H

#include <queue>
#include "page/b_plus_tree_page.h"

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 24
#define INTERNAL_PAGE_SIZE ((PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(MappingType)) - 1)
//能有多少个key（不包含INVALID KEY） 下标小于等于它就可以
/**
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
public:
  // must call initialize method after "create" a new node
  //初始化
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = INTERNAL_PAGE_SIZE);

  //给index 找key
  KeyType KeyAt(int index) const;

  //set index处的key
  void SetKeyAt(int index, const KeyType &key);

  //给value 找index
  int ValueIndex(const ValueType &value) const;

  //给index 找value
  ValueType ValueAt(int index) const;

  //给key 找value（return 0说明该key在这个中间节点中不存在）
  ValueType Lookup(const KeyType &key, const KeyComparator &comparator) const;

  //分裂产生一个新的根（新的根中有一个key：new_key  两个value：old_value<new_value)
  //只有InsertIntoParent()中会调用(b_plus_tree.cpp)
  void PopulateNewRoot(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value);
  
  //把new_value和new_key插入到old_value后面 (其余往后移)
  //（old_key,old_value) (new_key,new_key)
  int InsertNodeAfter(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value);

  //删除index处的key和value
  void Remove(int index);

  //该中间结点只有一对key和value，删除这对key和value并返回value
  ValueType RemoveAndReturnOnlyChild();

  // Split and Merge utility methods
  //把这个中间结点的所有内容move到recipient（另一中间结点：可能非空）
  //middle_key用来初始化最开始的key
  void MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key, BufferPoolManager *buffer_pool_manager);

  //把这个中间结点的后一半内容move到recipient（另一中间结点：可能非空）
  void MoveHalfTo(BPlusTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager);

  //把这个中间结点的第一个（key，value）移到recipient的最后
  void MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                        BufferPoolManager *buffer_pool_manager);
  //把这个中间结点的最后一个（key，value）移到recipient的最前面
  void MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                         BufferPoolManager *buffer_pool_manager);
  void MoveAllToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager);
  MappingType array_[0];
private:
  //items相当于一个pair的数组，把它搬到这个中间的结点的后面
  void CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager);

  //在中间结点的最后插入一对key和value，并根据value值找到哪个page页，设置它的爸爸
  void CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager);

  //在中间结点的最前面插入一对key和value（假设那个最前面的INVALID key也是有意义的值？）
  void CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager);

  //记录key和value对
  
};

#endif  // MINISQL_B_PLUS_TREE_INTERNAL_PAGE_H;
