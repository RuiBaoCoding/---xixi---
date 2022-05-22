#include "common/instance.h"
#include "gtest/gtest.h"
#include "index/b_plus_tree.h"
#include "index/basic_comparator.h"
#include "utils/tree_file_mgr.h"
#include "utils/utils.h"

static const std::string db_name = "bp_tree_insert_test.db";

TEST(BPlusTreeTests, SampleTest) {
  // Init engine
  DBStorageEngine engine(db_name);
  BasicComparator<int> comparator;
  BPlusTree<int, int, BasicComparator<int>> tree(0, engine.bpm_, comparator, 4, 4);
  TreeFileManagers mgr("tree_");
  // Prepare data
  const int n = 100;
  vector<int> keys;
  vector<int> values;
  vector<int> delete_seq;
  map<int, int> kv_map;
  for (int i = 0; i < n; i++) {
    keys.push_back(i);
    values.push_back(i);
    delete_seq.push_back(i);
  }
  
  int delete_num = n/2;
  //keys={23,34,14,31,11,1,37,28,43,25,8,7,27,2,49,18,42,40,29,35,32,22,45,5,4,16,47,46,48,30,26,21,10,36,15,20,13,24,17,38,19,33,41,3,44,12,39,0,9,6};
  //delete_seq={27,32,34,1,49,38,37,48,18,22,8,3,31,30,28,6,44,5,29,47,40,42,35,26,11,
  //43,21,10,36,12,16,14,25,4,45,17,46,13,33,39,41,7,2,19,9,23,24,15,0,20};
  // Shuffle data
  
  ShuffleArray(keys);
  ShuffleArray(values);
  ShuffleArray(delete_seq);
  // Map key value
  for (int i = 0; i < n; i++) {
    kv_map[keys[i]] = values[i];
  }
  //输出随机生成的数据
  /*cout<<"Insert:";
  for(int i = 0 ; i < n ; i ++ ){
    cout<<keys[i]<<",";
  }
  cout<<endl;
  cout<<"Is_Delete:";
  for (int i = 0 ; i < n/2 ; i ++){
    cout<<delete_seq[i]<<",";
  }
  cout<<endl;
  cout<<"Not_Delete:";
  for ( int i = n/2 ; i < n ; i ++ ){
    cout<<delete_seq[i]<<",";
  }
  cout<<endl;*/
  // Insert data
  for (int i = 0; i < n; i++) {
    //cout<<"insert i: "<<i<<endl;
    ASSERT_TRUE(tree.Insert(keys[i], values[i]));
  }
  ASSERT_TRUE(tree.Check());
  // Print tree
  tree.PrintTree(mgr[0]);
  std::cout<<"PrintTree end."<<std::endl;
  // Search keys
  
  vector<int> ans;
  for (int i = 0; i < n; i++) {
    tree.GetValue(i, ans);
    ASSERT_EQ(kv_map[i], ans[i]);
  }
  ASSERT_TRUE(tree.Check());
  // Delete half keys
  
  for (int i = 0; i < delete_num; i++) {
    cout<<"remove: "<<delete_seq[i]<<endl;
    /*cout<<"-----------------------"<<endl;
    tree.Remove(delete_seq[i]);
    cout<<"--------------------------"<<endl;
    for (auto p = pages.begin();p!=pages.end();p++){
      tree.Print(*p);
    }
    cout<<"--------------------------"<<endl;*/
    tree.Remove(delete_seq[i]);
  }
  vector <page_id_t> pages;
  pages={21,
      9,20,
      4,19,14,8,17,
      2,24,13,6,18,15,23,5,22,11,3,12,25,10,7,16};
  cout<<"*********************Page Print*************************"<<endl;
  for (auto m = pages.begin();m!=pages.end();m++){
    tree.Print(*m);
  }
  tree.PrintTree(mgr[1]);
  // Check valid
  
  ans.clear();
  for (int i = 0; i < delete_num; i++) {
    cout<<delete_seq[i]<<endl;
    ASSERT_FALSE(tree.GetValue(delete_seq[i], ans));
  }
  cout<<"---------------------------------"<<endl;
  for (int i = delete_num; i < n; i++) {
    cout<<delete_seq[i]<<endl;
    ASSERT_TRUE(tree.GetValue(delete_seq[i], ans));
    ASSERT_EQ(kv_map[delete_seq[i]], ans[ans.size() - 1]);
  }
}