#ifndef MINISQL_B_PLUS_TREE_INTERNAL_PAGE_H
#define MINISQL_B_PLUS_TREE_INTERNAL_PAGE_H

#include <queue>
#include "page/b_plus_tree_page.h"

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 24
#define INTERNAL_PAGE_SIZE ((PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(MappingType)) - 1)
//���ж��ٸ�key��������INVALID KEY�� �±�С�ڵ������Ϳ���
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
  //��ʼ��
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = INTERNAL_PAGE_SIZE);

  //��index ��key
  KeyType KeyAt(int index) const;

  //set index����key
  void SetKeyAt(int index, const KeyType &key);

  //��value ��index
  int ValueIndex(const ValueType &value) const;

  //��index ��value
  ValueType ValueAt(int index) const;

  //��key ��value��return 0˵����key������м�ڵ��в����ڣ�
  ValueType Lookup(const KeyType &key, const KeyComparator &comparator) const;

  //���Ѳ���һ���µĸ����µĸ�����һ��key��new_key  ����value��old_value<new_value)
  //ֻ��InsertIntoParent()�л����(b_plus_tree.cpp)
  void PopulateNewRoot(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value);
  
  //��new_value��new_key���뵽old_value���� (����������)
  //��old_key,old_value) (new_key,new_key)
  int InsertNodeAfter(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value);

  //ɾ��index����key��value
  void Remove(int index);

  //���м���ֻ��һ��key��value��ɾ�����key��value������value
  ValueType RemoveAndReturnOnlyChild();

  // Split and Merge utility methods
  //������м������������move��recipient����һ�м��㣺���ܷǿգ�
  //middle_key������ʼ���ʼ��key
  void MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key, BufferPoolManager *buffer_pool_manager);

  //������м���ĺ�һ������move��recipient����һ�м��㣺���ܷǿգ�
  void MoveHalfTo(BPlusTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager);

  //������м���ĵ�һ����key��value���Ƶ�recipient�����
  void MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                        BufferPoolManager *buffer_pool_manager);
  //������м�������һ����key��value���Ƶ�recipient����ǰ��
  void MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                         BufferPoolManager *buffer_pool_manager);
  void MoveAllToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager);
  MappingType array_[0];
private:
  //items�൱��һ��pair�����飬�����ᵽ����м�Ľ��ĺ���
  void CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager);

  //���м����������һ��key��value��������valueֵ�ҵ��ĸ�pageҳ���������İְ�
  void CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager);

  //���м������ǰ�����һ��key��value�������Ǹ���ǰ���INVALID keyҲ���������ֵ����
  void CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager);

  //��¼key��value��
  
};

#endif  // MINISQL_B_PLUS_TREE_INTERNAL_PAGE_H;
