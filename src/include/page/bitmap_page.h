#ifndef MINISQL_BITMAP_PAGE_H
#define MINISQL_BITMAP_PAGE_H

#include <bitset>

#include "common/macros.h"
#include "common/config.h"

/*
Bitmap Page Meta包括已经分配的页的数量以及下一个空闲的数据页
Bitmap Content存
bytes[]存某一页是否有空余
bytes中元素为char类型每一个char是两个十六进制数字，能够表示8个数据页的分布情况
因此表示的信息也是size*8
*/
template<size_t PageSize>
class BitmapPage {
public:
  /**
   * @return The number of pages that the bitmap page can record, i.e. the capacity of an extent.
   */
  static constexpr size_t GetMaxSupportedSize() { return 8 * MAX_CHARS; }

  /**
   * @param page_offset Index in extent of the page allocated.
   * @return true if successfully allocate a page.
   */
  bool AllocatePage(uint32_t &page_offset);

  /**
   * @return true if successfully de-allocate a page.
   */
  bool DeAllocatePage(uint32_t page_offset);

  /**
   * @return whether a page in the extent is free
   */
  bool IsPageFree(uint32_t page_offset) const;

private:
  /**
   * check a bit(byte_index, bit_index) in bytes is free(value 0).
   *
   * @param byte_index value of page_offset / 8
   * @param bit_index value of page_offset % 8
   * @return true if a bit is 0, false if 1.
   */
  bool IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const;

  /** Note: need to update if modify page structure. */
  static constexpr size_t MAX_CHARS = PageSize - 2 * sizeof(uint32_t);
  //默认bitmap page meta里存上述两个

private:
  /** The space occupied by all members of the class should be equal to the PageSize */
  [[maybe_unused]] uint32_t page_allocated_;
  [[maybe_unused]] uint32_t next_free_page_;
  [[maybe_unused]] unsigned char bytes[MAX_CHARS];
};

#endif //MINISQL_BITMAP_PAGE_H
