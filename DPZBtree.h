#pragma once
#include <cstdio>
#include <sys/stat.h>
#include <string.h>
#include <assert.h>
#include <vector>
#include <limits.h>
#include <cstdint>
#include <algorithm>
#include <random>
#include <ctime>
#include <sys/time.h>
#include <libpmemobj.h>
#include <libzbd/zbd.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>

#define GB 1024ULL*1024ULL*1024ULL
#define ZONE_PATH "/dev/nvme0n1"
#define LN_DEGREE   256 
#define IN_DEGREE   253 
#define ZNS_CAPACITY 100*GB
#define PM_CAPACITY 10*(ZNS_CAPACITY/100000)      
#define PAGE_SIZE 4096

unsigned long long PM_REMAIN = PM_CAPACITY;

class leaf_node;
class interna_node;
class BPlusTree;
class key_value;

leaf_node* firstleaf;


class key_value {
public:
	uint64_t key[LN_DEGREE];
	uint64_t value[LN_DEGREE];
};



class leaf_node {
public:
	bool hot; 
	interna_node* parent; 
	int nKeys; 
	key_value* pm_ptr; 
	uint64_t zone_ptr;
	leaf_node* LN; 
	leaf_node* RN;
	time_t timestamp; 
	uint64_t min_key;
	leaf_node() {
		nKeys = 0;
	}
};




class interna_node {
public:
	interna_node* parent; 
	int nKeys; 
	uint64_t key[IN_DEGREE]; 
	void* child[IN_DEGREE + 1]; 
	uint64_t reserved[3];
	interna_node() {
		nKeys = 0;
	}
};




std::vector<uint64_t> KMeans(const std::vector<uint64_t>& data, int k, int maxIterations) {
	int n = data.size();
	if (n == 0 || k <= 0 || maxIterations <= 0) {
		return {};
	}

	
	std::vector<uint64_t> centroids(k);
	srand(time(0));
	for (int i = 0; i < k; ++i) {
		centroids[i] = data[rand() % n];
	}

	std::vector<int> assignments(n);
	std::vector<__uint128_t> newCentroids(k); 
	std::vector<int> counts(k);

	for (int iter = 0; iter < maxIterations; ++iter) {
		
		for (int i = 0; i < n; ++i) {
			uint64_t minDist = std::numeric_limits<uint64_t>::max();
			int bestCluster = 0;
			for (int j = 0; j < k; ++j) {
				uint64_t dist = std::abs(static_cast<int64_t>(data[i]) - static_cast<int64_t>(centroids[j]));
				if (dist < minDist) {
					minDist = dist;
					bestCluster = j;
				}
			}
			assignments[i] = bestCluster;
		}

		
		std::fill(newCentroids.begin(), newCentroids.end(), 0);
		std::fill(counts.begin(), counts.end(), 0);
		for (int i = 0; i < n; ++i) {
			newCentroids[assignments[i]] += data[i];
			counts[assignments[i]]++;
		}
		for (int j = 0; j < k; ++j) {
			if (counts[j] > 0) {
				centroids[j] = static_cast<uint64_t>(newCentroids[j] / counts[j]);
			}
			else {
				centroids[j] = data[rand() % n];
			}
		}

		
		bool converged = true;
		for (int j = 0; j < k; ++j) {
			if (centroids[j] != newCentroids[j]) {
				converged = false;
				break;
			}
		}
		if (converged) {
			break;
		}
	}

	return centroids;
}





int read_f = -1;
int read_direct_f = -1;
int write_f = -1;


struct zbd_zone* zone_array;
unsigned int nr_zones;
uint32_t block_sz;


int openzone() {
	zbd_info info;
	int ret;

	read_f = zbd_open(ZONE_PATH, O_RDONLY, &info);
	if (read_f < 0) {
		perror("read_f fail line :118\n");
		return -1;
	}
	
	read_direct_f = zbd_open(ZONE_PATH, O_RDONLY | O_DIRECT, &info);
	if (read_direct_f < 0) {
		perror("read_direct_f fail line :124\n");
		return -1;
	}

	write_f = zbd_open(ZONE_PATH, O_WRONLY | O_DIRECT, &info);
	if (write_f < 0) {
		perror("write_f fail line :130\n");
		return -1;
	}

	ret = zbd_list_zones(read_f, 0, info.zone_size * info.nr_zones, ZBD_RO_ALL, &zone_array, &nr_zones);
	if (ret < 0) {
		perror("zbd_list_zones fail line :136\n");
		return -1;
	}
	block_sz = info.pblock_size;
	printf("block_sz is %lu\n", block_sz);
	printf("nr_zones is %lu\n", nr_zones);
	printf("create zone success\n");
	return 0;

}



bool ZoneIsWritable(zbd_zone* zone, unsigned int idx) {
	struct zbd_zone* z = &zone[idx];
	return !(zbd_zone_full(z) || zbd_zone_offline(z) || zbd_zone_rdonly(z));
};



bool ZoneIsSequential(zbd_zone* zone, unsigned int idx) {
	struct zbd_zone* z = &zone[idx];
	return zbd_zone_seq(z);
};


int get_writeble_zone(uint64_t size) {

	for (int i = 0; i < nr_zones; i++) {
		if (ZoneIsSequential(zone_array, i) == true && ZoneIsWritable(zone_array, i) == true &&
			zone_array[i].capacity >= size)
			return i;
	}

	return -1;
}

int Write(key_value* data, uint64_t size, uint64_t pos) {
	return pwrite(write_f, data, size, pos);
}





uint64_t Append(key_value* data, uint64_t size) {


	
	int number = get_writeble_zone(size);
	if (number < 0) {
		perror("NO Zone for write\n");
		return -1;
	}

	uint64_t write_pos = zone_array[number].wp;


	key_value* ptr = data;
	uint64_t left = size;
	int ret;


	
	assert((size % block_sz) == 0);
	while (left) {
		ret = Write(ptr, left, zone_array[number].wp);
		if (ret < 0) {
			perror("pwrite failed\n");
			fprintf(stderr, "Error code: %d\n", errno);
			return -1;
		}
		ptr += ret;
		zone_array[number].wp += ret;
		zone_array[number].capacity -= ret;
		left -= ret;
	}
	return write_pos;
}



int read_s(key_value* buf, int size, uint64_t pos) {
	return pread(read_direct_f, buf, size, pos);
}



int Read(key_value* buf, uint64_t offset, int n) {
	int ret = 0;
	int left = n;
	int r = -1;
	while (left) {
		r = read_s(buf, left, offset);
		if (r <= 0) {
			if (r == -1 && errno == EINTR) {
				continue;
			}
			break;
		}
		ret += r;
		buf += r;
		left -= r;
		offset += r;
	}
	if (r < 0) return r;
	return ret;
}







POBJ_LAYOUT_BEGIN(zbplustree);
POBJ_LAYOUT_TOID(zbplustree, key_value);
POBJ_LAYOUT_TOID(zbplustree, leaf_node);
POBJ_LAYOUT_END(zbplustree);
PMEMobjpool* pop;



int file_exists(const char* filename) {
	struct stat buffer;
	return stat(filename, &buffer);
}



void openPmemobjPool() {
	printf("use pmdk!\n");
	char pathname[100] = "/mnt/pmem/zbtree/pool";
	int sds_write_value = 0;
	pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);
	if (file_exists(pathname) != 0) {
		printf("create new one.\n");
		
		
		

		if ((pop = pmemobj_create(pathname, POBJ_LAYOUT_NAME(zbplustree),
			(uint64_t)1 * 1024 * 1024 * 1024, 0666)) ==
			NULL) {
			perror("failed to create pool.\n");
			return;
		}
		printf("pm create success\n");
	}
	else {
		printf("open existing one.\n");

		if ((pop = pmemobj_open(pathname, POBJ_LAYOUT_NAME(zbplustree))) == NULL) {
			perror("failed to open pool.\n");
			return;
		}
	}
}





class BPlusTree {
public:
	void* root;
	std::vector<leaf_node*>hot_list;
	int height;
	BPlusTree() {
		root = NULL;
		openzone();
		openPmemobjPool();
	}

	
	~BPlusTree() {
		pmemobj_close(pop);
		close(write_f);
		close(read_direct_f);
		close(read_f);
	}

	
	leaf_node* find_minTM_hotND() {
		leaf_node* node = hot_list[0];
		int j = 0;
		for (int i = 1; i < hot_list.size(); i++) {
			if (hot_list[i]->timestamp < node->timestamp)
			{
				node = hot_list[i];
				j = i;
			}
		}
		hot_list.erase(hot_list.begin() + j);
		return node;
	}

	
	leaf_node* find_notme_minTM_hotND(leaf_node* node) {
		leaf_node* node1;
		time_t min1 = hot_list[0]->timestamp;  
		time_t min2 = hot_list[1]->timestamp;  
		int min1_index = 0;
		int min2_index = 1;

		if (min1 > min2) {
			std::swap(min1, min2);  
			std::swap(min1_index, min2_index);

		}

		for (int i = 2; i < hot_list.size(); i++) {
			if (hot_list[i]->timestamp < min1) {
				min2 = min1;
				min2_index = min1_index;
				min1 = hot_list[i]->timestamp;
				min1_index = i;
			}
			else if (hot_list[i]->timestamp < min2 && hot_list[i]->timestamp != min1) {
				min2 = hot_list[i]->timestamp;
				min2_index = i;
			}
		}

		
		if (node == hot_list[min1_index]) {
			node1 = hot_list[min2_index];
			hot_list.erase(hot_list.begin() + min2_index);

		}

		else
		{
			node1 = hot_list[min1_index];
			hot_list.erase(hot_list.begin() + min1_index);
		}

		return node1;
	}



	
	int swap_hot_node(leaf_node* hot_node) {
		int errval = -1;
		
		leaf_node* victim = find_minTM_hotND();
		key_value* v1 = (key_value*)aligned_alloc(PAGE_SIZE, sizeof(key_value));

		
		for (int i = 0; i < victim->nKeys; i++) {
			v1->key[i] = victim->pm_ptr->key[i];
			v1->value[i] = victim->pm_ptr->value[i];
		}
		
		uint64_t pos = Append(v1, sizeof(key_value));
		
		PMEMoid oid;
		oid = pmemobj_oid(victim->pm_ptr);
		pmemobj_free(&oid);
		PM_REMAIN = PM_REMAIN + PAGE_SIZE;
		victim->pm_ptr = NULL;
		victim->hot = false;
		victim->zone_ptr = pos;
		pmemobj_persist(pop, victim, sizeof(leaf_node));

		
		key_value* v2 = (key_value*)aligned_alloc(PAGE_SIZE, sizeof(key_value));
		errval = Read(v2, hot_node->zone_ptr, sizeof(key_value));

		key_value* newvalue = allocKV();

		for (int i = 0; i < hot_node->nKeys; i++) {
			newvalue->key[i] = v2->key[i];
			newvalue->value[i] = v2->value[i];
		}
		pmemobj_persist(pop, newvalue, sizeof(key_value));

		hot_node->pm_ptr = newvalue;
		hot_node->hot = true;
		hot_node->timestamp = time(NULL);
		pmemobj_persist(pop, hot_node, sizeof(leaf_node));

		
		hot_list.push_back(hot_node);
		free(v1);
		free(v2);
		return 0;
	}


	
	int hot_node_dispose(uint64_t hot_key) {
		int errval = -1;
		
		if (root == NULL) {
			errval = create_first_node();
		}

		
		else {
			leaf_node* current;
			current = find_leaf(hot_key);

			
			if (current->hot == true) { errval = 0; }

			
			else {
				
				if (PM_REMAIN < PAGE_SIZE) {
					errval = swap_hot_node(current);
				}

				
				else {
					key_value* value = (key_value*)aligned_alloc(PAGE_SIZE, sizeof(key_value));
					errval = Read(value, current->zone_ptr, sizeof(key_value));

					key_value* v = allocKV();

					for (int i = 0; i < current->nKeys; i++) {
						v->key[i] = value->key[i];
						v->value[i] = value->value[i];
					}
					pmemobj_persist(pop, v, sizeof(key_value));

					current->pm_ptr = v;
					current->hot = true;
					current->timestamp = time(NULL);
					pmemobj_persist(pop, current, sizeof(leaf_node));

					
					hot_list.push_back(current);
					free(value);

				}
			}

		}
		return errval;
	}


	
	
	
	int create_first_node() {
		int errval = -1;
		leaf_node* newleaf = allocLN();

		key_value* newvalue = allocKV();

		
		newleaf->pm_ptr = newvalue;
		newleaf->hot = true;
		newleaf->LN = NULL;
		newleaf->RN = NULL;
		newleaf->parent = NULL;
		newleaf->timestamp = time(NULL);
		pmemobj_persist(pop, newleaf, sizeof(leaf_node));
		
		root = newleaf;
		height = 1;
		
		hot_list.push_back(newleaf);
		firstleaf = newleaf;
		return 0;
	}


	
	leaf_node* find_leaf(uint64_t key) {
		void* currentNode = root;
		if (currentNode == NULL) {
			perror("root is null\n");
			return NULL;
		}

		for (int i = height; i > 1; i--) {
			
			interna_node* internalNode = (interna_node*)currentNode;
			
			if (internalNode->nKeys == 0) {
				currentNode = internalNode->child[0];
			}
			else {
				int j = 0;
				while (j < internalNode->nKeys && key >= internalNode->key[j]) {
					j++;
				}
				currentNode = internalNode->child[j];
			}
		}
		return (leaf_node*)currentNode;
	}


	leaf_node* allocLN() {
		TOID(leaf_node) p;
		POBJ_ZNEW(pop, &p, leaf_node);
		if (TOID_IS_NULL(p)) {
			perror("allocLN fail\n");
			return NULL;
		}
		return D_RW(p); 
	}


	key_value* allocKV() {
		TOID(key_value) p;
		POBJ_ZNEW(pop, &p, key_value);
		if (TOID_IS_NULL(p)) {
			perror("allocKV fail\n");
			return NULL;
		}
		PM_REMAIN = PM_REMAIN - PAGE_SIZE;
		return D_RW(p); 

	}


	
	
	
	int binarySearchForInsert(uint64_t arr[], unsigned int size, uint64_t target) {
		int left = 0;
		int right = size - 1;

		while (left <= right) {
			int mid = left + (right - left) / 2;

			if (arr[mid] == target) {
				
				return mid;
			}
			else if (arr[mid] < target) {
				
				left = mid + 1;
			}
			else {
				
				right = mid - 1;
			}
		}

		
		return left;
	}



	
	void insertElement(uint64_t arr[], unsigned int size, uint64_t target, int position) {
		if (size == 0) {
			
			arr[0] = target;
		}
		else {
			
			for (int i = size; i > position; i--) {
				arr[i] = arr[i - 1];
			}
			arr[position] = target;
		}

	}



	
	int insert(uint64_t key, uint64_t value) {
		leaf_node* LN;
		int errval = -1;
		LN = find_leaf(key);

		
		if (LN->nKeys < LN_DEGREE) {
			
			if (LN->hot == true) {
				
				int p = binarySearchForInsert(LN->pm_ptr->key, LN->nKeys, key);
				
				insertElement(LN->pm_ptr->key, LN->nKeys, key, p);
				insertElement(LN->pm_ptr->value, LN->nKeys, value, p);
				
				pmemobj_persist(pop, LN->pm_ptr, sizeof(key_value));
				
				LN->nKeys++;
				LN->timestamp = time(NULL);
				
				pmemobj_persist(pop, LN, sizeof(leaf_node));
			}

			
			else {
				key_value* value1 = (key_value*)aligned_alloc(PAGE_SIZE, sizeof(key_value));
				errval = Read(value1, LN->zone_ptr, sizeof(key_value));

				
				int p = binarySearchForInsert(value1->key, LN->nKeys, key);
				
				insertElement(value1->key, LN->nKeys, key, p);
				insertElement(value1->value, LN->nKeys, value, p);
				
				uint64_t pos = Append(value1, sizeof(key_value));
				
				LN->nKeys++;
				LN->timestamp = time(NULL);
				LN->zone_ptr = pos;
				pmemobj_persist(pop, LN, sizeof(leaf_node));
				free(value1);
			}

		}

		
		else {
			split_leaf_node(LN);
			insert(key, value);
		}

		return 0;
	}


	
	int delete_hot_node(leaf_node* node) {
		leaf_node* victim = find_notme_minTM_hotND(node);
		key_value* v1 = (key_value*)aligned_alloc(PAGE_SIZE, sizeof(key_value));

		
		for (int i = 0; i < victim->nKeys; i++) {
			v1->key[i] = victim->pm_ptr->key[i];
			v1->value[i] = victim->pm_ptr->value[i];
		}
		
		uint64_t pos = Append(v1, sizeof(key_value));
		
		PMEMoid oid;
		oid = pmemobj_oid(victim->pm_ptr);
		pmemobj_free(&oid);
		PM_REMAIN = PM_REMAIN + PAGE_SIZE;
		victim->pm_ptr = NULL;
		victim->hot = false;
		victim->zone_ptr = pos;
		pmemobj_persist(pop, victim, sizeof(leaf_node));
		free(v1);
		return 0;
	}



	
	void split_leaf_node(leaf_node* node) {
		
		leaf_node* newLeafNode = allocLN();


		
		if (node->hot == true) {
			
			if (PM_REMAIN < PAGE_SIZE) {
				delete_hot_node(node);
			}
			key_value* newvalue = allocKV();

			
			int splitPoint = node->nKeys / 2;
			
			for (int i = splitPoint; i < node->nKeys; i++) {
				newvalue->value[i - splitPoint] = node->pm_ptr->value[i];
				newvalue->key[i - splitPoint] = node->pm_ptr->key[i];
			}

			pmemobj_persist(pop, newvalue, sizeof(key_value));


			
			
			newLeafNode->nKeys = node->nKeys - splitPoint;
			newLeafNode->hot = true;
			newLeafNode->timestamp = time(NULL);
			newLeafNode->pm_ptr = newvalue;
			newLeafNode->RN = node->RN;
			newLeafNode->LN = node;
			newLeafNode->min_key = newvalue->key[0];
			if (node->RN != NULL) {
				node->RN->LN = newLeafNode;
				pmemobj_persist(pop, node->RN, sizeof(leaf_node));
			}
			node->RN = newLeafNode;
			node->nKeys = splitPoint;
			node->timestamp = time(NULL);
			node->min_key = node->pm_ptr->key[0];
			pmemobj_persist(pop, newLeafNode, sizeof(key_value));
			pmemobj_persist(pop, node, sizeof(leaf_node));
			
			hot_list.push_back(newLeafNode);
		}


		
		else {
			
			if (PM_REMAIN < PAGE_SIZE * 2) {
				delete_hot_node(node);
				delete_hot_node(node);
			}
			key_value* buf = (key_value*)aligned_alloc(PAGE_SIZE, sizeof(key_value));
			key_value* newvalue1 = allocKV();
			key_value* newvalue2 = allocKV();


			
			int splitPoint = node->nKeys / 2;
			
			int errval = Read(buf, node->zone_ptr, sizeof(key_value));

			for (int i = 0; i < splitPoint; i++) {
				newvalue1->key[i] = buf->key[i];
				newvalue1->value[i] = buf->value[i];
			}
			for (int i = splitPoint; i < node->nKeys; i++) {
				newvalue2->key[i - splitPoint] = buf->key[i];
				newvalue2->value[i - splitPoint] = buf->value[i];
			}
			pmemobj_persist(pop, newvalue1, sizeof(key_value));
			pmemobj_persist(pop, newvalue2, sizeof(key_value));


			
			newLeafNode->nKeys = node->nKeys - splitPoint;
			newLeafNode->hot = true;
			newLeafNode->timestamp = time(NULL);
			newLeafNode->pm_ptr = newvalue2;
			newLeafNode->RN = node->RN;
			newLeafNode->LN = node;
			newLeafNode->min_key = newvalue2->key[0];
			if (node->RN != NULL) {
				node->RN->LN = newLeafNode;
				pmemobj_persist(pop, node->RN, sizeof(leaf_node));
			}
			node->RN = newLeafNode;
			node->hot = true;
			node->nKeys = splitPoint;
			node->timestamp = time(NULL);
			node->pm_ptr = newvalue1;
			node->min_key = newvalue1->key[0];
			pmemobj_persist(pop, newLeafNode, sizeof(leaf_node));
			pmemobj_persist(pop, node, sizeof(leaf_node));
			
			hot_list.push_back(newLeafNode);
			hot_list.push_back(node);
			free(buf);
		}


		
		insert_into_parent(node, newLeafNode->pm_ptr->key[0], newLeafNode);
	}

	
	void insert_into_parent(leaf_node* leftChild, uint64_t key, leaf_node* rightChild) {
		interna_node* parent = leftChild->parent;

		if (parent == NULL) {
			
			interna_node* newRoot = (interna_node*)aligned_alloc(PAGE_SIZE, sizeof(interna_node));
			newRoot->key[0] = key;
			newRoot->child[0] = leftChild;
			newRoot->child[1] = rightChild;
			newRoot->nKeys = 1;
			newRoot->parent = NULL;

			leftChild->parent = newRoot;
			rightChild->parent = newRoot;
			pmemobj_persist(pop, leftChild, sizeof(leaf_node));
			pmemobj_persist(pop, rightChild, sizeof(leaf_node));
			root = newRoot; 
			height++;
		}

		
		else if (parent->nKeys == 0) {
			parent->key[0] = key;
			parent->child[1] = rightChild;
			parent->nKeys = 1;
			rightChild->parent = parent;
			pmemobj_persist(pop, rightChild, sizeof(leaf_node));
		}

		
		else {
			
			if (parent->nKeys < IN_DEGREE) {
				
				int insertIndex = binarySearchForInsert(parent->key, parent->nKeys, key);

				
				for (int i = parent->nKeys; i > insertIndex; i--) {
					parent->key[i] = parent->key[i - 1];
					parent->child[i + 1] = parent->child[i];
				}

				parent->key[insertIndex] = key;
				parent->child[insertIndex + 1] = rightChild; 
				parent->nKeys++;
				rightChild->parent = parent;

				pmemobj_persist(pop, rightChild, sizeof(leaf_node));


			}
			else {
				split_interna_node(parent);
				insert_into_parent(leftChild, key, rightChild);
			}
		}
	}

	
	interna_node* split_interna_node(interna_node* node) {
		
		interna_node* newInternaNode = (interna_node*)aligned_alloc(PAGE_SIZE, sizeof(interna_node));

		
		int splitPoint = (node->nKeys) / 2;

		
		for (int i = splitPoint + 1; i < node->nKeys; i++) {
			newInternaNode->key[i - (splitPoint + 1)] = node->key[i];
		}
		
		for (int i = splitPoint + 1; i < node->nKeys + 1; i++) {
			newInternaNode->child[i - (splitPoint + 1)] = node->child[i];
		}
		
		int j = find_node_height(node);
		if (j == 2) {
			for (int i = splitPoint + 1; i < node->nKeys + 1; i++) {
				leaf_node* leaf = (leaf_node*)(node->child[i]);
				leaf->parent = newInternaNode;
			}
		}
		else {
			for (int i = splitPoint + 1; i < node->nKeys + 1; i++) {
				interna_node* inter = (interna_node*)(node->child[i]);
				inter->parent = newInternaNode;
			}
		}

		
		newInternaNode->nKeys = node->nKeys - splitPoint - 1;
		node->nKeys = splitPoint;

		
		insert_into_parent(node, node->key[splitPoint], newInternaNode);
		return newInternaNode;
	}



	
	void insert_into_parent(interna_node* leftChild, uint64_t key, interna_node* rightChild) {
		interna_node* parent = leftChild->parent;

		if (parent == NULL) {
			
			interna_node* newRoot = (interna_node*)aligned_alloc(PAGE_SIZE, sizeof(interna_node));
			newRoot->key[0] = key;
			newRoot->child[0] = leftChild;
			newRoot->child[1] = rightChild;
			newRoot->nKeys = 1;
			newRoot->parent = NULL;

			leftChild->parent = newRoot;
			rightChild->parent = newRoot;
			root = newRoot; 
			height++;
		}

		
		else if (parent->nKeys == 0) {
			parent->key[0] = key;
			parent->child[1] = rightChild;
			parent->nKeys = 1;
			rightChild->parent = parent;
		}


		
		else {
			
			if (parent->nKeys < IN_DEGREE) {
				
				int insertIndex = binarySearchForInsert(parent->key, parent->nKeys, key);

				
				for (int i = parent->nKeys; i > insertIndex; i--) {
					parent->key[i] = parent->key[i - 1];
					parent->child[i + 1] = parent->child[i];
				}

				parent->key[insertIndex] = key;
				parent->child[insertIndex + 1] = rightChild; 
				parent->nKeys++;
				rightChild->parent = parent;
			}
			else {
				
				split_interna_node(parent);
				insert_into_parent(leftChild, key, rightChild);
			}
		}
	}

	int find_node_height(interna_node* node) {
		int h;
		interna_node* current = node;
		for (int i = 0; i < height; i++) {
			if (current->parent == NULL) {
				h = height - i;
				break;
			}
			current = current->parent;
		}
		return h;

	}

	
	
	int handleParent(interna_node* node) {
		
		
		interna_node* parent = node->parent;
		interna_node* left_bro;
		interna_node* right_bro;
		
		for (int i = 0; i < parent->nKeys + 1; i++) {
			interna_node* pt = (interna_node*)parent->child[i];
			
			if (pt == node) {
				
				if (i == 0) {
					right_bro = (interna_node*)parent->child[i + 1];
					
					if (right_bro->nKeys > IN_DEGREE / 2) {
						node->child[node->nKeys + 1] = right_bro->child[0];
						node->key[node->nKeys] = parent->key[i];
						parent->key[i] = right_bro->key[0];
						for (int j = 0; j < right_bro->nKeys; j++) {
							right_bro->child[j] = right_bro->child[j + 1];
						}

						for (int j = 0; j < right_bro->nKeys - 1; j++) {
							right_bro->key[j] = right_bro->key[j + 1];
						}
						right_bro->nKeys--;
						node->nKeys++;

						
						int h = find_node_height(node);
						if (h == 2) {
							leaf_node* leaf = (leaf_node*)(node->child[node->nKeys]);
							leaf->parent = node;
						}
						else {
							interna_node* inter = (interna_node*)(node->child[node->nKeys]);
							inter->parent = node;
						}
						return 0;
					}

					
					else {
						
						node->key[node->nKeys] = parent->key[i];
						
						for (int j = node->nKeys + 1; j < node->nKeys + right_bro->nKeys + 1; j++) {
							node->key[j] = right_bro->key[j - node->nKeys - 1];
						}
						for (int j = node->nKeys + 1; j < node->nKeys + right_bro->nKeys + 2; j++) {
							node->child[j] = right_bro->child[j - node->nKeys - 1];
						}
						
						int h = find_node_height(node);
						if (h == 2) {
							for (int j = node->nKeys + 1; j < node->nKeys + right_bro->nKeys + 2; j++) {
								leaf_node* leaf = (leaf_node*)(node->child[j]);
								leaf->parent = node;
							}
						}
						else {
							for (int j = node->nKeys + 1; j < node->nKeys + right_bro->nKeys + 2; j++) {
								interna_node* inter = (interna_node*)(node->child[j]);
								inter->parent = node;
							}
						}
						node->nKeys = right_bro->nKeys + node->nKeys + 1;
						
						for (int k = i; k < parent->nKeys - 1; k++) {
							parent->key[k] = parent->key[k + 1];
						}
						for (int k = i + 1; k < parent->nKeys; k++) {
							parent->child[k] = parent->child[k + 1];
						}
						parent->nKeys--;
						
						if (parent->nKeys < IN_DEGREE / 2) {
							
							if (parent->parent == NULL) {
								if (parent->nKeys == 0) {
									node->parent = NULL;
									root = node;
									height--;
								}
								return 0;
							}
							
							else {
								handleParent(parent);
								return 0;
							}
						}
						
						return 0;
					}

				}
				
				else {
					left_bro = (interna_node*)parent->child[i - 1];
					
					if (left_bro->nKeys > IN_DEGREE / 2) {
						for (int j = node->nKeys + 1; j > 0; j--) {
							node->child[j] = node->child[j - 1];
						}
						for (int j = node->nKeys; j > 0; j--) {
							node->key[j] = node->key[j - 1];
						}
						node->child[0] = left_bro->child[left_bro->nKeys];
						node->key[0] = parent->key[i - 1];
						parent->key[i - 1] = left_bro->key[left_bro->nKeys - 1];
						left_bro->nKeys--;
						node->nKeys++;
						
						int h = find_node_height(node);
						if (h == 2) {
							leaf_node* leaf = (leaf_node*)(node->child[0]);
							leaf->parent = node;
						}
						else {
							interna_node* inter = (interna_node*)(node->child[0]);
							inter->parent = node;
						}
						return 0;
					}

					
					else {
						
						left_bro->key[left_bro->nKeys] = parent->key[i - 1];
						
						for (int j = left_bro->nKeys + 1; j < left_bro->nKeys + node->nKeys + 1; j++) {
							left_bro->key[j] = node->key[j - left_bro->nKeys - 1];
						}
						for (int j = left_bro->nKeys + 1; j < left_bro->nKeys + node->nKeys + 2; j++) {
							left_bro->child[j] = node->child[j - left_bro->nKeys - 1];
						}
						
						int h = find_node_height(node);
						if (h == 2) {
							for (int j = left_bro->nKeys + 1; j < node->nKeys + left_bro->nKeys + 2; j++) {
								leaf_node* leaf = (leaf_node*)(left_bro->child[j]);
								leaf->parent = left_bro;
							}
						}
						else {
							for (int j = left_bro->nKeys + 1; j < node->nKeys + left_bro->nKeys + 2; j++) {
								interna_node* inter = (interna_node*)(left_bro->child[j]);
								inter->parent = left_bro;
							}
						}

						left_bro->nKeys = left_bro->nKeys + node->nKeys + 1;
						
						for (int k = i - 1; k < parent->nKeys - 1; k++) {
							parent->key[k] = parent->key[k + 1];
						}
						for (int k = i; k < parent->nKeys; k++) {
							parent->child[k] = parent->child[k + 1];
						}
						parent->nKeys--;
						
						if (parent->nKeys < IN_DEGREE / 2) {
							
							if (parent->parent == NULL) {
								if (parent->nKeys == 0) {
									left_bro->parent = NULL;
									root = left_bro;
									height--;
								}
								return 0;
							}
							
							else {
								handleParent(parent);
								return 0;
							}
						}
						
						return 0;
					}
				}
			}
		}
	}

	


	int binarySearch(uint64_t arr[], unsigned int size, uint64_t target) {
		int left = 0;
		int right = size - 1;

		while (left <= right) {
			int mid = left + (right - left) / 2;

			if (arr[mid] == target) {
				
				return mid;
			}
			else if (arr[mid] < target) {
				
				left = mid + 1;
			}
			else {
				
				right = mid - 1;
			}
		}

		
		return -1;
	}

	
	
	int Update(uint64_t key, uint64_t value)
	{
		int errval = -1;
		leaf_node* current = find_leaf(key);
		
		if (current->hot == true) {
			int pos = binarySearch(current->pm_ptr->key, current->nKeys, key);
			
			if (pos < 0) { return errval; }
			current->pm_ptr->value[pos] = value;
			current->timestamp = time(NULL);
			pmemobj_persist(pop, current, sizeof(leaf_node));
			errval = 0;
		}
		
		else {
			key_value* value1 = (key_value*)aligned_alloc(PAGE_SIZE, sizeof(key_value));
			errval = Read(value1, current->zone_ptr, sizeof(key_value));

			int pos = binarySearch(value1->key, current->nKeys, key);
			
			if (pos < 0) { return errval; }
			value1->value[pos] = value;
			uint64_t pos1 = Append(value1, sizeof(key_value));
			
			current->timestamp = time(NULL);
			current->zone_ptr = pos1;
			pmemobj_persist(pop, current, sizeof(leaf_node));
			errval = 0;
			free(value1);
		}
		return errval;
	}




	
	int Delete(uint64_t key) {
		leaf_node* leaf = find_leaf(key);
		
		if (leaf->hot == true) {
			int p = -1;
			
			p = binarySearch(leaf->pm_ptr->key, leaf->nKeys, key);
			if (p == -1) {
				return -1;
			}

			
			if (leaf->parent == NULL) {
				
				for (int i = p; i < leaf->nKeys - 1; i++) {
					leaf->pm_ptr->key[i] = leaf->pm_ptr->key[i + 1];
					leaf->pm_ptr->value[i] = leaf->pm_ptr->value[i + 1];
				}
				leaf->nKeys--;
				leaf->timestamp = time(NULL);
				
				if (leaf->nKeys == 0) {
					root = leaf;
					leaf->parent = NULL;
					height = 1;
					
					pmemobj_persist(pop, leaf, sizeof(leaf_node));
					pmemobj_persist(pop, leaf->pm_ptr, sizeof(key_value));
					printf("delete all\n");
					return 0;
				}
				
				pmemobj_persist(pop, leaf, sizeof(leaf_node));
				pmemobj_persist(pop, leaf->pm_ptr, sizeof(key_value));
				return 0;
			}

			
			
			if (leaf->nKeys > LN_DEGREE / 2) {
				
				for (int i = p; i < leaf->nKeys - 1; i++) {
					leaf->pm_ptr->key[i] = leaf->pm_ptr->key[i + 1];
					leaf->pm_ptr->value[i] = leaf->pm_ptr->value[i + 1];
				}
				leaf->nKeys--;
				leaf->timestamp = time(NULL);
				
				pmemobj_persist(pop, leaf, sizeof(leaf_node));
				pmemobj_persist(pop, leaf->pm_ptr, sizeof(key_value));
				return 0;
			}

			
			else {
				for (int i = 0; i < leaf->parent->nKeys + 1; i++) {
					if (leaf->parent->child[i] == leaf) {
						
						if (i == 0) {
							
							for (int i = p; i < leaf->nKeys - 1; i++) {
								leaf->pm_ptr->key[i] = leaf->pm_ptr->key[i + 1];
								leaf->pm_ptr->value[i] = leaf->pm_ptr->value[i + 1];
							}
							leaf->nKeys--;
							
							if (leaf->RN->nKeys > LN_DEGREE / 2) {
									
									if (leaf->RN->hot == true) {
										leaf->pm_ptr->key[leaf->nKeys] = leaf->RN->pm_ptr->key[0];
										leaf->pm_ptr->value[leaf->nKeys] = leaf->RN->pm_ptr->value[0];
										for (int k = 0; k < leaf->RN->nKeys - 1; k++) {
											leaf->RN->pm_ptr->key[k] = leaf->RN->pm_ptr->key[k + 1];
											leaf->RN->pm_ptr->value[k] = leaf->RN->pm_ptr->value[k + 1];
										}
										leaf->nKeys++;
										leaf->RN->nKeys--;
										
										for (int i = 0; i < leaf->parent->nKeys + 1; i++) {
											if (leaf->parent->child[i] == leaf) {
												leaf->parent->key[i] = leaf->RN->pm_ptr->key[0];
												break;
											}
										}
										leaf->timestamp = time(NULL);
										pmemobj_persist(pop, leaf, sizeof(leaf_node));
										pmemobj_persist(pop, leaf->pm_ptr, sizeof(key_value));
										pmemobj_persist(pop, leaf->RN, sizeof(leaf_node));
										pmemobj_persist(pop, leaf->RN->pm_ptr, sizeof(key_value));
										return 0;
									}


									
									else {
										
										int err = -1;
										key_value* value2 = (key_value*)aligned_alloc(PAGE_SIZE, sizeof(key_value));
										err = Read(value2, leaf->RN->zone_ptr, sizeof(key_value));
										
										leaf->pm_ptr->key[leaf->nKeys] = value2->key[0];
										leaf->pm_ptr->value[leaf->nKeys] = value2->value[0];
										for (int k = 0; k < leaf->RN->nKeys - 1; k++) {
											value2->key[k] = value2->key[k + 1];
											value2->value[k] = value2->value[k + 1];
										}
										leaf->nKeys++;
										leaf->RN->nKeys--;

										
										uint64_t wp2 = Append(value2, sizeof(key_value));
										
										for (int i = 0; i < leaf->parent->nKeys + 1; i++) {
											if (leaf->parent->child[i] == leaf) {
												leaf->parent->key[i] = value2->key[0];
												break;
											}
										}
										leaf->RN->zone_ptr = wp2;
										leaf->timestamp = time(NULL);
										pmemobj_persist(pop, leaf, sizeof(leaf_node));
										pmemobj_persist(pop, leaf->pm_ptr, sizeof(key_value));
										pmemobj_persist(pop, leaf->RN, sizeof(leaf_node));
										free(value2);
										return 0;
									}

								}


							
							
							else {
									
									if (leaf->RN->hot == true) {
										
										for (int k = leaf->nKeys; k < leaf->nKeys + leaf->RN->nKeys; k++) {
											leaf->pm_ptr->key[k] = leaf->RN->pm_ptr->key[k - leaf->nKeys];
											leaf->pm_ptr->value[k] = leaf->RN->pm_ptr->value[k - leaf->nKeys];
										}
										leaf->nKeys = leaf->nKeys + leaf->RN->nKeys;
										
										for (int i = 0; i < hot_list.size(); i++)
											if (hot_list[i] == leaf->RN)
											{
												hot_list.erase(hot_list.begin() + i);
												break;
											}
										
										PMEMoid oid;
										oid = pmemobj_oid(leaf->RN->pm_ptr);
										pmemobj_free(&oid);
										PM_REMAIN = PM_REMAIN + PAGE_SIZE;
										
										if (leaf->RN->RN != NULL) {
											leaf->RN->RN->LN = leaf;
											leaf->RN = leaf->RN->RN;

											pmemobj_persist(pop, leaf, sizeof(leaf_node));
											pmemobj_persist(pop, leaf->pm_ptr, sizeof(key_value));
											pmemobj_persist(pop, leaf->RN, sizeof(leaf_node));
										}
										else {
											leaf->RN = NULL;

											pmemobj_persist(pop, leaf, sizeof(leaf_node));
											pmemobj_persist(pop, leaf->pm_ptr, sizeof(key_value));
										}
										
										for (int i = 0; i < leaf->parent->nKeys + 1; i++) {
											if (leaf->parent->child[i] == leaf) {
												for (int m = i; m < leaf->parent->nKeys - 1; m++) {
													leaf->parent->key[m] = leaf->parent->key[m + 1];
												}
												for (int n = i + 1; n < leaf->parent->nKeys; n++) {
													leaf->parent->child[n] = leaf->parent->child[n + 1];
												}
												goto la;
											}
										}
									la:
										leaf->parent->nKeys--;
										
										if (leaf->parent->nKeys < IN_DEGREE / 2) {
											interna_node* node = leaf->parent;
											
											if (node->parent == NULL) {
												
												if (node->nKeys == 0) {
													leaf->parent = NULL;
													root = leaf;
													height = 1;
												}
												return 0;
											}
											
											else {
												handleParent(node);
												return 0;
											}
										}

										
										return 0;
									}

									
									else {
										
										int errval = -1;
										key_value* value2 = (key_value*)aligned_alloc(PAGE_SIZE, sizeof(key_value));
										errval = Read(value2, leaf->RN->zone_ptr, sizeof(key_value));
										

										for (int k = leaf->nKeys; k < leaf->nKeys + leaf->RN->nKeys; k++) {
											leaf->pm_ptr->key[k] = value2->key[k - leaf->nKeys];
											leaf->pm_ptr->value[k] = value2->value[k - leaf->nKeys];
										}
										leaf->nKeys = leaf->nKeys + leaf->RN->nKeys;
										if (leaf->RN->RN != NULL) {
											leaf->RN->RN->LN = leaf;
											leaf->RN = leaf->RN->RN;

											leaf->timestamp = time(NULL);
											
											pmemobj_persist(pop, leaf, sizeof(leaf_node));
											pmemobj_persist(pop, leaf->pm_ptr, sizeof(key_value));
										}
										else {
											leaf->RN = NULL;

											leaf->timestamp = time(NULL);
											
											pmemobj_persist(pop, leaf, sizeof(leaf_node));
											pmemobj_persist(pop, leaf->pm_ptr, sizeof(key_value));;
										}
										
										for (int i = 0; i < leaf->parent->nKeys + 1; i++) {
											if (leaf->parent->child[i] == leaf) {
												for (int m = i; m < leaf->parent->nKeys - 1; m++) {
													leaf->parent->key[m] = leaf->parent->key[m + 1];
												}
												for (int n = i + 1; n < leaf->parent->nKeys; n++) {
													leaf->parent->child[n] = leaf->parent->child[n + 1];
												}
												goto lb;;
											}
										}
									lb:
										leaf->parent->nKeys--;
										
										if (leaf->parent->nKeys < IN_DEGREE / 2) {
											interna_node* node = leaf->parent;

											
											if (node->parent == NULL) {
												
												if (node->nKeys == 0) {
													leaf->parent = NULL;
													root = leaf;
													height = 1;
												}
												free(value2);
												return 0;
											}
											
											else {
												handleParent(node);
												free(value2);
												return 0;
											}
										}

										
										free(value2);
										return 0;
									}

								}
							
						}
						
						
						else {
							
							for (int i = p; i < leaf->nKeys - 1; i++) {
								leaf->pm_ptr->key[i] = leaf->pm_ptr->key[i + 1];
								leaf->pm_ptr->value[i] = leaf->pm_ptr->value[i + 1];
							}
							leaf->nKeys--;

							
							if (leaf->LN->nKeys > LN_DEGREE / 2) {
								
								if (leaf->LN->hot == true) {
									for (int k = leaf->nKeys; k > 0; k--) {
										leaf->pm_ptr->key[k] = leaf->pm_ptr->key[k - 1];
										leaf->pm_ptr->value[k] = leaf->pm_ptr->value[k - 1];
									}
									leaf->pm_ptr->key[0] = leaf->LN->pm_ptr->key[leaf->LN->nKeys - 1];
									leaf->pm_ptr->value[0] = leaf->LN->pm_ptr->value[leaf->LN->nKeys - 1];
									leaf->nKeys++;
									leaf->LN->nKeys--;
									
									for (int i = 0; i < leaf->parent->nKeys + 1; i++) {
										if (leaf->parent->child[i] == leaf) {
											leaf->parent->key[i - 1] = leaf->pm_ptr->key[0];
											break;
										}
									}
									leaf->timestamp = time(NULL);
									pmemobj_persist(pop, leaf, sizeof(leaf_node));
									pmemobj_persist(pop, leaf->pm_ptr, sizeof(leaf_node));
									pmemobj_persist(pop, leaf->LN, sizeof(leaf_node));
									pmemobj_persist(pop, leaf->LN->pm_ptr, sizeof(key_value));
									return 0;
								}

								
								else {
									
									int err = -1;
									key_value* value2 = (key_value*)aligned_alloc(PAGE_SIZE, sizeof(key_value));
									err = Read(value2, leaf->LN->zone_ptr, sizeof(key_value));
									
									for (int k = leaf->nKeys; k > 0; k--) {
										leaf->pm_ptr->key[k] = leaf->pm_ptr->key[k - 1];
										leaf->pm_ptr->value[k] = leaf->pm_ptr->value[k - 1];
									}
									leaf->pm_ptr->key[0] = value2->key[leaf->LN->nKeys - 1];
									leaf->pm_ptr->value[0] = value2->value[leaf->LN->nKeys - 1];
									leaf->nKeys++;
									leaf->LN->nKeys--;

									
									uint64_t wp2 = Append(value2, sizeof(key_value));
									
									for (int i = 0; i < leaf->parent->nKeys + 1; i++) {
										if (leaf->parent->child[i] == leaf) {
											leaf->parent->key[i - 1] = leaf->pm_ptr->key[0];
											break;
										}
									}
									leaf->LN->zone_ptr = wp2;
									leaf->timestamp = time(NULL);
									pmemobj_persist(pop, leaf, sizeof(leaf_node));
									pmemobj_persist(pop, leaf->pm_ptr, sizeof(leaf_node));
									pmemobj_persist(pop, leaf->LN, sizeof(leaf_node));
									free(value2);
									return 0;
								}
							}

							
							
							else {
								
								if (leaf->LN->hot == true) {
									
									for (int k = leaf->LN->nKeys; k < leaf->nKeys + leaf->LN->nKeys; k++) {
										leaf->LN->pm_ptr->key[k] = leaf->pm_ptr->key[k - leaf->LN->nKeys];
										leaf->LN->pm_ptr->value[k] = leaf->pm_ptr->value[k - leaf->LN->nKeys];
									}

									leaf->LN->nKeys = leaf->nKeys + leaf->LN->nKeys;

									PMEMoid oid;
									oid = pmemobj_oid(leaf->pm_ptr);
									pmemobj_free(&oid);
									PM_REMAIN = PM_REMAIN + PAGE_SIZE;


									if (leaf->RN != NULL) {
										leaf->LN->RN = leaf->RN;
										leaf->RN->LN = leaf->LN;

										leaf->LN->timestamp = time(NULL);
										
										pmemobj_persist(pop, leaf->LN, sizeof(leaf_node));
										pmemobj_persist(pop, leaf->RN, sizeof(leaf_node));
										pmemobj_persist(pop, leaf->LN->pm_ptr, sizeof(key_value));
									}
									else {
										leaf->LN->RN = NULL;

										leaf->LN->timestamp = time(NULL);
										
										pmemobj_persist(pop, leaf->LN, sizeof(leaf_node));
										pmemobj_persist(pop, leaf->LN->pm_ptr, sizeof(key_value));
									}
									
									for (int i = 0; i < hot_list.size(); i++)
										if (hot_list[i] == leaf)
										{
											hot_list.erase(hot_list.begin() + i);
											break;
										}
									
									for (int i = 0; i < leaf->parent->nKeys + 1; i++) {
										if (leaf->parent->child[i] == leaf) {
											for (int m = i - 1; m < leaf->parent->nKeys - 1; m++) {
												leaf->parent->key[m] = leaf->parent->key[m + 1];
											}
											for (int n = i; n < leaf->parent->nKeys; n++) {
												leaf->parent->child[n] = leaf->parent->child[n + 1];
											}
											goto lc;
										}
									}
								lc:
									leaf->parent->nKeys--;
									
									if (leaf->parent->nKeys < IN_DEGREE / 2) {
										interna_node* node = leaf->parent;
										
										if (node->parent == NULL) {
											
											if (node->nKeys == 0) {
												leaf->LN->parent = NULL;
												root = leaf->LN;
												height = 1;
											}
											return 0;
										}
										
										else {
											handleParent(node);
											return 0;
										}
									}

									
									return 0;
								}

								
								else {
									
									int errval = -1;
									key_value* value2 = (key_value*)aligned_alloc(PAGE_SIZE, sizeof(key_value));
									errval = Read(value2, leaf->LN->zone_ptr, sizeof(key_value));

									key_value* newvalue1 = allocKV();

									for (int k = 0; k < leaf->LN->nKeys; k++) {
										newvalue1->key[k] = value2->key[k];
										newvalue1->value[k] = value2->value[k];
									}
									for (int k = leaf->LN->nKeys; k < leaf->nKeys + leaf->LN->nKeys; k++) {
										newvalue1->key[k] = leaf->pm_ptr->key[k - leaf->LN->nKeys];
										newvalue1->value[k] = leaf->pm_ptr->value[k - leaf->LN->nKeys];
									}
									leaf->LN->nKeys = leaf->nKeys + leaf->LN->nKeys;
									leaf->LN->pm_ptr = newvalue1;
									leaf->LN->hot = true;
									if (leaf->RN != NULL) {
										leaf->LN->RN = leaf->RN;
										leaf->RN->LN = leaf->LN;

										leaf->LN->timestamp = time(NULL);
										
										pmemobj_persist(pop, leaf->LN, sizeof(leaf_node));
										pmemobj_persist(pop, leaf->LN->pm_ptr, sizeof(leaf_node));
									}
									else {
										leaf->LN->RN = NULL;

										leaf->LN->timestamp = time(NULL);
										
										pmemobj_persist(pop, leaf->LN, sizeof(leaf_node));
										pmemobj_persist(pop, leaf->LN->pm_ptr, sizeof(leaf_node));
									}
									
									for (int i = 0; i < hot_list.size(); i++)
										if (hot_list[i] == leaf)
										{
											hot_list.erase(hot_list.begin() + i);
											break;
										}
									
									PMEMoid oid;
									oid = pmemobj_oid(leaf->pm_ptr);
									pmemobj_free(&oid);
									PM_REMAIN = PM_REMAIN + PAGE_SIZE;

									hot_list.push_back(leaf->LN);

									
									for (int i = 0; i < leaf->parent->nKeys + 1; i++) {
										if (leaf->parent->child[i] == leaf) {
											for (int m = i - 1; m < leaf->parent->nKeys - 1; m++) {
												leaf->parent->key[m] = leaf->parent->key[m + 1];
											}
											for (int n = i; n < leaf->parent->nKeys; n++) {
												leaf->parent->child[n] = leaf->parent->child[n + 1];
											}
											break;
										}
									}
									leaf->parent->nKeys--;
									
									if (leaf->parent->nKeys < IN_DEGREE / 2) {
										interna_node* node = leaf->parent;

										
										if (node->parent == NULL) {
											
											if (node->nKeys == 0) {
												leaf->LN->parent = NULL;
												root = leaf->LN;
												height = 1;
											}
											free(value2);
											return 0;
										}
										
										else {
											handleParent(node);
											free(value2);
											return 0;
										}
									}

									
									free(value2);
									return 0;
								}
							}
							
						}
					}
				}
				
			}
		}

		
		else {
			
			int errval = -1;
			key_value* value1 = (key_value*)aligned_alloc(PAGE_SIZE, sizeof(key_value));
			errval = Read(value1, leaf->zone_ptr, sizeof(key_value));
			

			int p = -1;
			
			p = binarySearch(value1->key, leaf->nKeys, key);
			if (p == -1) {
				free(value1);
				return -1;
			}

			
			if (leaf->parent == NULL) {
				
				for (int i = p; i < leaf->nKeys - 1; i++) {
					value1->key[i] = value1->key[i + 1];
					value1->value[i] = value1->value[i + 1];
				}
				leaf->nKeys--;
				
				if (leaf->nKeys == 0) {
					root = leaf;
					height = 1;
					
					key_value* newvalue = allocKV();
					
					leaf->hot = true;
					leaf->parent = NULL;
					leaf->LN = NULL;
					leaf->RN = NULL;
					leaf->pm_ptr = newvalue;
					leaf->timestamp = time(NULL);
					
					pmemobj_persist(pop, leaf, sizeof(leaf_node));
					pmemobj_persist(pop, leaf->pm_ptr, sizeof(key_value));
					
					hot_list.push_back(leaf);
					free(value1);
					printf("delete all\n");
					return 0;
				}

				
				uint64_t pos = Append(value1, sizeof(key_value));

				leaf->zone_ptr = pos;
				leaf->timestamp = time(NULL);
				pmemobj_persist(pop, leaf, sizeof(leaf_node));
				free(value1);
				return 0;
			}

			
			
			if (leaf->nKeys > LN_DEGREE / 2) {
				
				if (PM_REMAIN < PAGE_SIZE) {
					key_value* newvalue = allocKV();
					
					
					for (int i = p; i < leaf->nKeys - 1; i++) {
						value1->key[i] = value1->key[i + 1];
						value1->value[i] = value1->value[i + 1];
					}
					leaf->nKeys--;
					
					for (int i = 0; i < leaf->nKeys; i++) {
						newvalue->key[i] = value1->key[i];
						newvalue->value[i] = value1->value[i];
					}
					leaf->hot = true;
					leaf->pm_ptr = newvalue;
					leaf->timestamp = time(NULL);
					hot_list.push_back(leaf);
					pmemobj_persist(pop, leaf, sizeof(leaf_node));
					pmemobj_persist(pop, leaf->pm_ptr, sizeof(key_value));
					return 0;
				}
				
				for (int i = p; i < leaf->nKeys - 1; i++) {
					value1->key[i] = value1->key[i + 1];
					value1->value[i] = value1->value[i + 1];
				}
		
				
				uint64_t pos = Append(value1, sizeof(key_value));

				
				leaf->nKeys--;
				leaf->zone_ptr = pos;
				leaf->timestamp = time(NULL);
				pmemobj_persist(pop, leaf, sizeof(leaf_node));
				free(value1);
				return 0;

			}

			
			else {
				for (int i = 0; i < leaf->parent->nKeys + 1; i++) {
					if (leaf->parent->child[i] == leaf) {
						if (i == 0) {
							
							for (int j = p; j < leaf->nKeys - 1; j++) {
								value1->key[j] = value1->key[j + 1];
								value1->value[j] = value1->value[j + 1];
							}
							leaf->nKeys--;

							
							if (leaf->RN->nKeys > LN_DEGREE / 2) {
									
									if (leaf->RN->hot == true) {
										
										if (PM_REMAIN < PAGE_SIZE) {
											key_value* newvalue = allocKV();
											if (newvalue == NULL) {
												perror("allocKV fail\n");
											}
											
											for (int i = 0; i < leaf->nKeys; i++) {
												newvalue->key[i] = value1->key[i];
												newvalue->value[i] = value1->value[i];
											}
											newvalue->key[leaf->nKeys] = leaf->RN->pm_ptr->key[0];
											newvalue->value[leaf->nKeys] = leaf->RN->pm_ptr->value[0];
											for (int k = 0; k < leaf->RN->nKeys - 1; k++) {
												leaf->RN->pm_ptr->key[k] = leaf->RN->pm_ptr->key[k + 1];
												leaf->RN->pm_ptr->value[k] = leaf->RN->pm_ptr->value[k + 1];
											}
											leaf->nKeys++;
											leaf->RN->nKeys--;
											leaf->hot = true;
											leaf->pm_ptr = newvalue;
											leaf->timestamp = time(NULL);
											hot_list.push_back(leaf);
											
											for (int i = 0; i < leaf->parent->nKeys + 1; i++) {
												if (leaf->parent->child[i] == leaf) {
													leaf->parent->key[i] = leaf->RN->pm_ptr->key[0];
													break;
												}
											}
											pmemobj_persist(pop, leaf, sizeof(leaf_node));
											pmemobj_persist(pop, leaf->pm_ptr, sizeof(key_value));
											pmemobj_persist(pop, leaf->RN, sizeof(leaf_node));
											pmemobj_persist(pop, leaf->RN->pm_ptr, sizeof(key_value));
											free(value1);
											return 0;
										}

										value1->key[leaf->nKeys] = leaf->RN->pm_ptr->key[0];
										value1->value[leaf->nKeys] = leaf->RN->pm_ptr->value[0];
										for (int k = 0; k < leaf->RN->nKeys - 1; k++) {
											leaf->RN->pm_ptr->key[k] = leaf->RN->pm_ptr->key[k + 1];
											leaf->RN->pm_ptr->value[k] = leaf->RN->pm_ptr->value[k + 1];
										}
										leaf->nKeys++;
										leaf->RN->nKeys--;
										
										uint64_t wp1 = Append(value1, sizeof(key_value));
										leaf->zone_ptr = wp1;
										leaf->timestamp = time(NULL);
										
										for (int i = 0; i < leaf->parent->nKeys + 1; i++) {
											if (leaf->parent->child[i] == leaf) {
												leaf->parent->key[i] = leaf->RN->pm_ptr->key[0];
												break;
											}
										}
										pmemobj_persist(pop, leaf, sizeof(leaf_node));
										pmemobj_persist(pop, leaf->RN, sizeof(leaf_node));
										pmemobj_persist(pop, leaf->RN->pm_ptr, sizeof(key_value));
										free(value1);
										return 0;
									}

									
									else {
										
										int err = -1;
										key_value* value2 = (key_value*)aligned_alloc(PAGE_SIZE, sizeof(key_value));
										err = Read(value2, leaf->RN->zone_ptr, sizeof(key_value));
										if (err < 0) {
											printf("readFileToArray fail\n");
											return err;
										}
										
										if (PM_REMAIN < PAGE_SIZE) {
											key_value* newvalue = allocKV();
											if (newvalue == NULL) {
												perror("allocKV fail\n");
											}
											
											for (int i = 0; i < leaf->nKeys; i++) {
												newvalue->key[i] = value1->key[i];
												newvalue->value[i] = value1->value[i];
											}
											newvalue->key[leaf->nKeys] = value2->key[0];
											newvalue->value[leaf->nKeys] = value2->value[0];
											for (int k = 0; k < leaf->RN->nKeys - 1; k++) {
												value2->key[k] = value2->key[k + 1];
												value2->value[k] = value2->value[k + 1];
											}
											leaf->nKeys++;
											leaf->RN->nKeys--;

											
											uint64_t wp2 = Append(value2, sizeof(key_value));
											leaf->RN->zone_ptr = wp2;
											leaf->hot = true;
											leaf->pm_ptr = newvalue;
											leaf->timestamp = time(NULL);
											hot_list.push_back(leaf);
											
											for (int i = 0; i < leaf->parent->nKeys + 1; i++) {
												if (leaf->parent->child[i] == leaf) {
													leaf->parent->key[i] = value2->key[0];
													break;
												}
											}
											pmemobj_persist(pop, leaf, sizeof(leaf_node));
											pmemobj_persist(pop, leaf->pm_ptr, sizeof(key_value));
											pmemobj_persist(pop, leaf->RN, sizeof(leaf_node));
											free(value1);
											free(value2);
											return 0;

										}
										
										value1->key[leaf->nKeys] = value2->key[0];
										value1->value[leaf->nKeys] = value2->value[0];
										for (int k = 0; k < leaf->RN->nKeys - 1; k++) {
											value2->key[k] = value2->key[k + 1];
											value2->value[k] = value2->value[k + 1];
										}
										leaf->nKeys++;
										leaf->RN->nKeys--;

										
										uint64_t wp1 = Append(value1, sizeof(key_value));
										uint64_t wp2 = Append(value2, sizeof(key_value));
										leaf->zone_ptr = wp1;
										leaf->RN->zone_ptr = wp2;
										leaf->timestamp = time(NULL);
										
										for (int i = 0; i < leaf->parent->nKeys + 1; i++) {
											if (leaf->parent->child[i] == leaf) {
												leaf->parent->key[i] = value2->key[0];
												break;
											}
										}
										pmemobj_persist(pop, leaf, sizeof(leaf_node));
										pmemobj_persist(pop, leaf->RN, sizeof(leaf_node));
										free(value1);
										free(value2);
										return 0;
									}

								}


							
							
							else {
									
									if (leaf->RN->hot == true) {
										key_value* newvalue1 = allocKV();
										if (newvalue1 == NULL) {
											perror("allocKV fail\n");
										}
										for (int k = 0; k < leaf->nKeys; k++) {
											newvalue1->key[k] = value1->key[k];
											newvalue1->value[k] = value1->value[k];
										}
										for (int k = leaf->nKeys; k < leaf->nKeys + leaf->RN->nKeys; k++) {
											newvalue1->key[k] = leaf->RN->pm_ptr->key[k - leaf->nKeys];
											newvalue1->value[k] = leaf->RN->pm_ptr->value[k - leaf->nKeys];
										}
										leaf->nKeys = leaf->nKeys + leaf->RN->nKeys;
										leaf->pm_ptr = newvalue1;
										leaf->hot = true;
										
										for (int i = 0; i < hot_list.size(); i++)
											if (hot_list[i] == leaf->RN)
											{
												hot_list.erase(hot_list.begin() + i);
												break;
											}

										PMEMoid oid;
										oid = pmemobj_oid(leaf->RN->pm_ptr);
										pmemobj_free(&oid);
										PM_REMAIN = PM_REMAIN + PAGE_SIZE;

										hot_list.push_back(leaf);
										if (leaf->RN->RN != NULL) {
											leaf->RN->RN->LN = leaf;
											leaf->RN = leaf->RN->RN;

											leaf->timestamp = time(NULL);
											pmemobj_persist(pop, leaf, sizeof(leaf_node));
											pmemobj_persist(pop, leaf->RN, sizeof(leaf_node));
											pmemobj_persist(pop, leaf->pm_ptr, sizeof(key_value));
										}
										else {
											leaf->RN = NULL;

											leaf->timestamp = time(NULL);
											pmemobj_persist(pop, leaf, sizeof(leaf_node));
											pmemobj_persist(pop, leaf->pm_ptr, sizeof(key_value));
										}
										
										for (int i = 0; i < leaf->parent->nKeys + 1; i++) {
											if (leaf->parent->child[i] == leaf) {
												for (int m = i; m < leaf->parent->nKeys - 1; m++) {
													leaf->parent->key[m] = leaf->parent->key[m + 1];
												}
												for (int n = i + 1; n < leaf->parent->nKeys; n++) {
													leaf->parent->child[n] = leaf->parent->child[n + 1];
												}
												break;
											}
										}
										leaf->parent->nKeys--;
										
										if (leaf->parent->nKeys < IN_DEGREE / 2) {
											interna_node* node = leaf->parent;

											
											if (node->parent == NULL) {
												
												if (node->nKeys == 0) {
													leaf->parent = NULL;
													root = leaf;
													height = 1;
												}
												free(value1);
												return 0;
											}
											
											else {
												handleParent(node);
												free(value1);
												return 0;
											}
										}

										
										free(value1);
										return 0;
									}

									
									else {
										int errval = -1;
										key_value* value2 = (key_value*)aligned_alloc(PAGE_SIZE, sizeof(key_value));
										errval = Read(value2, leaf->RN->zone_ptr, sizeof(key_value));
										if (errval < 0) {
											printf("readFileToArray fail\n");
											return errval;
										}
										
										if (PM_REMAIN < PAGE_SIZE) {
											key_value* newvalue = allocKV();
											if (newvalue == NULL) {
												perror("allocKV fail\n");
											}
											
											for (int i = 0; i < leaf->nKeys; i++) {
												newvalue->key[i] = value1->key[i];
												newvalue->value[i] = value1->value[i];
											}
											for (int k = leaf->nKeys; k < leaf->nKeys + leaf->RN->nKeys; k++) {
												newvalue->key[k] = value2->key[k - leaf->nKeys];
												newvalue->value[k] = value2->value[k - leaf->nKeys];
											}
											leaf->nKeys = leaf->nKeys + leaf->RN->nKeys;

											if (leaf->RN->RN != NULL) {
												leaf->RN->RN->LN = leaf;
												leaf->RN = leaf->RN->RN;

												leaf->timestamp = time(NULL);
												leaf->hot = true;
												leaf->pm_ptr = newvalue;
												
												pmemobj_persist(pop, leaf, sizeof(leaf_node));
												pmemobj_persist(pop, leaf->pm_ptr, sizeof(key_value));
												pmemobj_persist(pop, leaf->RN, sizeof(leaf_node));
											}
											else {
												leaf->RN = NULL;

												leaf->timestamp = time(NULL);
												leaf->hot = true;
												leaf->pm_ptr = newvalue;
												
												pmemobj_persist(pop, leaf, sizeof(leaf_node));
												pmemobj_persist(pop, leaf->pm_ptr, sizeof(key_value));
											}
											hot_list.push_back(leaf);
											
											for (int i = 0; i < leaf->parent->nKeys + 1; i++) {
												if (leaf->parent->child[i] == leaf) {
													for (int m = i; m < leaf->parent->nKeys - 1; m++) {
														leaf->parent->key[m] = leaf->parent->key[m + 1];
													}
													for (int n = i + 1; n < leaf->parent->nKeys; n++) {
														leaf->parent->child[n] = leaf->parent->child[n + 1];
													}
													break;
												}
											}
											leaf->parent->nKeys--;

											if (leaf->parent->nKeys < IN_DEGREE / 2) {
												interna_node* node = leaf->parent;

												
												if (node->parent == NULL) {
													
													if (node->nKeys == 0) {
														leaf->parent = NULL;
														root = leaf;
														height = 1;
													}
													free(value1);
													free(value2);
													return 0;
												}
												
												else {
													handleParent(node);
													free(value1);
													free(value2);
													return 0;
												}
											}
											free(value1);
											free(value2);
											return 0;
										}
										
										
										for (int k = leaf->nKeys; k < leaf->nKeys + leaf->RN->nKeys; k++) {
											value1->key[k] = value2->key[k - leaf->nKeys];
											value1->value[k] = value2->value[k - leaf->nKeys];
										}
										leaf->nKeys = leaf->nKeys + leaf->RN->nKeys;
										
										uint64_t wp1 = Append(value1, sizeof(key_value));
										leaf->zone_ptr = wp1;

										if (leaf->RN->RN != NULL) {
											leaf->RN->RN->LN = leaf;
											leaf->RN = leaf->RN->RN;

											leaf->timestamp = time(NULL);
											
											pmemobj_persist(pop, leaf, sizeof(leaf_node));
											pmemobj_persist(pop, leaf->RN, sizeof(leaf_node));
										}
										else {
											leaf->RN = NULL;

											leaf->timestamp = time(NULL);
											
											pmemobj_persist(pop, leaf, sizeof(leaf_node));
										}

										
										for (int i = 0; i < leaf->parent->nKeys + 1; i++) {
											if (leaf->parent->child[i] == leaf) {
												for (int m = i; m < leaf->parent->nKeys - 1; m++) {
													leaf->parent->key[m] = leaf->parent->key[m + 1];
												}
												for (int n = i + 1; n < leaf->parent->nKeys; n++) {
													leaf->parent->child[n] = leaf->parent->child[n + 1];
												}
												break;
											}
										}
										leaf->parent->nKeys--;
										
										if (leaf->parent->nKeys < IN_DEGREE / 2) {
											interna_node* node = leaf->parent;

											
											if (node->parent == NULL) {
												
												if (node->nKeys == 0) {
													leaf->parent = NULL;
													root = leaf;
													height = 1;
												}
												free(value1);
												free(value2);
												return 0;
											}
											
											else {
												handleParent(node);
												free(value1);
												free(value2);
												return 0;
											}
										}

										
										free(value1);
										free(value2);
										return 0;
									}

								}
							
						}

						else {
							
							for (int j = p; j < leaf->nKeys - 1; j++) {
								value1->key[j] = value1->key[j + 1];
								value1->value[j] = value1->value[j + 1];
							}
							leaf->nKeys--;

							
							if (leaf->LN->nKeys > LN_DEGREE / 2) {
								
								if (leaf->LN->hot == true) {
									
									if (PM_REMAIN < PAGE_SIZE) {
										key_value* newvalue = allocKV();
										if (newvalue == NULL) {
											perror("allocKV fail\n");
										}
										
										for (int i = 1; i < leaf->nKeys + 1; i++) {
											newvalue->key[i] = value1->key[i - 1];
											newvalue->value[i] = value1->value[i - 1];
										}
										newvalue->key[0] = leaf->LN->pm_ptr->key[leaf->LN->nKeys - 1];
										newvalue->value[0] = leaf->LN->pm_ptr->value[leaf->LN->nKeys - 1];
										leaf->nKeys++;
										leaf->LN->nKeys--;
										leaf->hot = true;
										leaf->pm_ptr = newvalue;
										leaf->timestamp = time(NULL);
										hot_list.push_back(leaf);
										
										for (int i = 0; i < leaf->parent->nKeys + 1; i++) {
											if (leaf->parent->child[i] == leaf) {
												leaf->parent->key[i - 1] = newvalue->key[0];
												break;
											}
										}
										pmemobj_persist(pop, leaf, sizeof(leaf_node));
										pmemobj_persist(pop, leaf->pm_ptr, sizeof(key_value));
										pmemobj_persist(pop, leaf->LN, sizeof(leaf_node));
										pmemobj_persist(pop, leaf->LN->pm_ptr, sizeof(key_value));
										free(value1);
										return 0;
									}

									for (int k = leaf->nKeys; k > 0; k--) {
										value1->key[k] = value1->key[k - 1];
										value1->value[k] = value1->value[k - 1];
									}
									value1->key[0] = leaf->LN->pm_ptr->key[leaf->LN->nKeys - 1];
									value1->value[0] = leaf->LN->pm_ptr->value[leaf->LN->nKeys - 1];
									leaf->nKeys++;
									leaf->LN->nKeys--;
									
									uint64_t wp1 = Append(value1, sizeof(key_value));
									
									for (int i = 0; i < leaf->parent->nKeys + 1; i++) {
										if (leaf->parent->child[i] == leaf) {
											leaf->parent->key[i - 1] = value1->key[0];
											break;
										}
									}
									leaf->zone_ptr = wp1;
									leaf->timestamp = time(NULL);
									pmemobj_persist(pop, leaf, sizeof(leaf_node));
									pmemobj_persist(pop, leaf->LN, sizeof(leaf_node));
									pmemobj_persist(pop, leaf->LN->pm_ptr, sizeof(key_value));
									free(value1);
									return 0;
								}

								
								else {
									
									int err = -1;
									key_value* value2 = (key_value*)aligned_alloc(PAGE_SIZE, sizeof(key_value));
									err = Read(value2, leaf->LN->zone_ptr, sizeof(key_value));
									if (err < 0) {
										printf("readFileToArray fail\n");
										return err;
									}

									
									if (PM_REMAIN < PAGE_SIZE) {
										key_value* newvalue = allocKV();
										if (newvalue == NULL) {
											perror("allocKV fail\n");
										}
										
										for (int i = 1; i < leaf->nKeys + 1; i++) {
											newvalue->key[i] = value1->key[i - 1];
											newvalue->value[i] = value1->value[i - 1];
										}
										newvalue->key[0] = value2->key[leaf->LN->nKeys - 1];
										newvalue->value[0] = value2->value[leaf->LN->nKeys - 1];
										leaf->nKeys++;
										leaf->LN->nKeys--;
										leaf->hot = true;
										leaf->pm_ptr = newvalue;
										leaf->timestamp = time(NULL);
										hot_list.push_back(leaf);
										
										uint64_t wp2 = Append(value2, sizeof(key_value));
										leaf->LN->zone_ptr = wp2;
										
										for (int i = 0; i < leaf->parent->nKeys + 1; i++) {
											if (leaf->parent->child[i] == leaf) {
												leaf->parent->key[i - 1] = newvalue->key[0];
												break;
											}
										}
										pmemobj_persist(pop, leaf, sizeof(leaf_node));
										pmemobj_persist(pop, leaf->pm_ptr, sizeof(key_value));
										pmemobj_persist(pop, leaf->LN, sizeof(leaf_node));
										return 0;
									}
									
									for (int k = leaf->nKeys; k > 0; k--) {
										value1->key[k] = value1->key[k - 1];
										value1->value[k] = value1->value[k - 1];
									}
									value1->key[0] = value2->key[leaf->LN->nKeys - 1];
									value1->value[0] = value2->value[leaf->LN->nKeys - 1];
									leaf->nKeys++;
									leaf->LN->nKeys--;

									
									uint64_t wp1 = Append(value1, sizeof(key_value));
									uint64_t wp2 = Append(value2, sizeof(key_value));
									
									for (int i = 0; i < leaf->parent->nKeys + 1; i++) {
										if (leaf->parent->child[i] == leaf) {
											leaf->parent->key[i - 1] = value1->key[0];
											break;
										}
									}
									leaf->zone_ptr = wp1;
									leaf->LN->zone_ptr = wp2;
									leaf->timestamp = time(NULL);
									pmemobj_persist(pop, leaf, sizeof(leaf_node));
									pmemobj_persist(pop, leaf->LN, sizeof(leaf_node));
									free(value1);
									free(value2);
									return 0;
								}
							}

							
							
							else {
								
								if (leaf->LN->hot == true) {
									
									for (int k = leaf->LN->nKeys; k < leaf->nKeys + leaf->LN->nKeys; k++) {
										leaf->LN->pm_ptr->key[k] = value1->key[k - leaf->LN->nKeys];
										leaf->LN->pm_ptr->value[k] = value1->value[k - leaf->LN->nKeys];
									}

									leaf->LN->nKeys = leaf->nKeys + leaf->LN->nKeys;

									if (leaf->RN != NULL) {
										leaf->LN->RN = leaf->RN;
										leaf->RN->LN = leaf->LN;

										leaf->LN->timestamp = time(NULL);
										
										pmemobj_persist(pop, leaf->LN, sizeof(leaf_node));
										pmemobj_persist(pop, leaf->RN, sizeof(leaf_node));
										pmemobj_persist(pop, leaf->LN->pm_ptr, sizeof(key_value));
									}
									else {
										leaf->LN->RN = NULL;
										leaf->LN->timestamp = time(NULL);
										
										pmemobj_persist(pop, leaf->LN, sizeof(leaf_node));
										pmemobj_persist(pop, leaf->LN->pm_ptr, sizeof(key_value));
									}
									
									for (int i = 0; i < leaf->parent->nKeys + 1; i++) {
										if (leaf->parent->child[i] == leaf) {
											for (int m = i - 1; m < leaf->parent->nKeys - 1; m++) {
												leaf->parent->key[m] = leaf->parent->key[m + 1];
											}
											for (int n = i; n < leaf->parent->nKeys; n++) {
												leaf->parent->child[n] = leaf->parent->child[n + 1];
											}
											break;
										}
									}
									leaf->parent->nKeys--;
									
									if (leaf->parent->nKeys < IN_DEGREE / 2) {
										interna_node* node = leaf->parent;

										
										if (node->parent == NULL) {
											
											if (node->nKeys == 0) {
												leaf->LN->parent = NULL;
												root = leaf->LN;
												height = 1;
											}
											free(value1);
											return 0;
										}
										
										else {
											handleParent(node);
											free(value1);
											return 0;
										}
									}

									
									free(value1);
									return 0;
								}

								
								else {
									int errval = -1;
									key_value* value2 = (key_value*)aligned_alloc(PAGE_SIZE, sizeof(key_value));
									errval = Read(value2, leaf->LN->zone_ptr, sizeof(key_value));
									if (errval < 0) {
										printf("readFileToArray fail\n");
										return errval;
									}

									
									if (PM_REMAIN < PAGE_SIZE) {
										key_value* newvalue = allocKV();
										if (newvalue == NULL) {
											perror("allocKV fail\n");
										}
										
										for (int i = 0; i < leaf->LN->nKeys; i++) {
											newvalue->key[i] = value2->key[i];
											newvalue->value[i] = value2->value[i];
										}
										for (int k = leaf->LN->nKeys; k < leaf->nKeys + leaf->LN->nKeys; k++) {
											newvalue->key[k] = value1->key[k - leaf->LN->nKeys];
											newvalue->value[k] = value1->value[k - leaf->LN->nKeys];
										}
										leaf->LN->nKeys = leaf->nKeys + leaf->LN->nKeys;

										if (leaf->RN != NULL) {
											leaf->LN->RN = leaf->RN;
											leaf->RN->LN = leaf->LN;

											leaf->LN->hot = true;
											leaf->LN->pm_ptr = newvalue;
											leaf->LN->timestamp = time(NULL);
											
											pmemobj_persist(pop, leaf->LN, sizeof(leaf_node));
											pmemobj_persist(pop, leaf->LN->pm_ptr, sizeof(key_value));
											pmemobj_persist(pop, leaf->RN, sizeof(leaf_node));
										}
										else {
											leaf->LN->RN = NULL;

											leaf->LN->hot = true;
											leaf->LN->pm_ptr = newvalue;
											leaf->LN->timestamp = time(NULL);
											
											pmemobj_persist(pop, leaf->LN, sizeof(leaf_node));
											pmemobj_persist(pop, leaf->LN->pm_ptr, sizeof(key_value));
										}
										hot_list.push_back(leaf->LN);
										
										for (int i = 0; i < leaf->parent->nKeys + 1; i++) {
											if (leaf->parent->child[i] == leaf) {
												for (int m = i - 1; m < leaf->parent->nKeys - 1; m++) {
													leaf->parent->key[m] = leaf->parent->key[m + 1];
												}
												for (int n = i; n < leaf->parent->nKeys; n++) {
													leaf->parent->child[n] = leaf->parent->child[n + 1];
												}
												break;
											}
										}
										leaf->parent->nKeys--;
										if (leaf->parent->nKeys < IN_DEGREE / 2) {
											interna_node* node = leaf->parent;

											
											if (node->parent == NULL) {
												
												if (node->nKeys == 0) {
													leaf->LN->parent = NULL;
													root = leaf->LN;
													height = 1;
												}
												free(value1);
												free(value2);
												return 0;
											}
											
											else {
												handleParent(node);
												free(value1);
												free(value2);
												return 0;
											}
										}
										free(value1);
										free(value2);
										return 0;
									}

									
									for (int k = leaf->LN->nKeys; k < leaf->nKeys + leaf->LN->nKeys; k++) {
										value2->key[k] = value1->key[k - leaf->LN->nKeys];
										value2->value[k] = value1->value[k - leaf->LN->nKeys];
									}
									leaf->LN->nKeys = leaf->nKeys + leaf->LN->nKeys;
									
									uint64_t wp1 = Append(value2, sizeof(key_value));
									leaf->LN->zone_ptr = wp1;
									if (leaf->RN != NULL) {
										leaf->LN->RN = leaf->RN;
										leaf->RN->LN = leaf->LN;
										leaf->LN->timestamp = time(NULL);
										
										pmemobj_persist(pop, leaf->LN, sizeof(leaf_node));
										pmemobj_persist(pop, leaf->RN, sizeof(leaf_node));
									}
									else {
										leaf->LN->RN = NULL;
										leaf->LN->timestamp = time(NULL);
										
										pmemobj_persist(pop, leaf->LN, sizeof(leaf_node));
									}
									
									for (int i = 0; i < leaf->parent->nKeys + 1; i++) {
										if (leaf->parent->child[i] == leaf) {
											for (int m = i - 1; m < leaf->parent->nKeys - 1; m++) {
												leaf->parent->key[m] = leaf->parent->key[m + 1];
											}
											for (int n = i; n < leaf->parent->nKeys; n++) {
												leaf->parent->child[n] = leaf->parent->child[n + 1];
											}
											break;
										}
									}
									leaf->parent->nKeys--;
									
									if (leaf->parent->nKeys < IN_DEGREE / 2) {
										interna_node* node = leaf->parent;

										
										if (node->parent == NULL) {
											
											if (node->nKeys == 0) {
												leaf->LN->parent = NULL;
												root = leaf->LN;
												height = 1;
											}
											free(value1);
											free(value2);
											return 0;
										}
										
										else {
											handleParent(node);
											free(value1);
											free(value2);
											return 0;
										}
									}

									
									free(value1);
									free(value2);
									return 0;
								}
							}


						}
					}
				}
			
			}

		}
	}

	
	uint64_t search(uint64_t key) {
		int errval = 0;
		leaf_node* current = find_leaf(key);
		
		if (current->hot == true) {
			int pos = binarySearch(current->pm_ptr->key, current->nKeys, key);
			
			if (pos < 0) {
				
				return errval;
			}
			current->timestamp = time(NULL);
			pmemobj_persist(pop, current, sizeof(leaf_node));
			return current->pm_ptr->value[pos];
		}
		
		else {
			key_value* value1 = (key_value*)aligned_alloc(PAGE_SIZE, sizeof(key_value));
			errval = Read(value1, current->zone_ptr, sizeof(key_value));
			if (errval < 0) {
				perror("readFileToArray fail\n");
				return errval;
			}
			int pos = binarySearch(value1->key, current->nKeys, key);
			
			if (pos < 0) {
				
				free(value1);
				return errval;
			}
			current->timestamp = time(NULL);
			pmemobj_persist(pop, current, sizeof(leaf_node));
			uint64_t v = value1->value[pos];
			free(value1);
			return v;
		}

	}

	
	std::vector<uint64_t> Scan(uint64_t s_key, long len) {
		std::vector<uint64_t> result;
		long remain = len;
		leaf_node* current = find_leaf(s_key);

		if (current->hot == true) {
			int pos = binarySearchForInsert(current->pm_ptr->key, current->nKeys, s_key);
			while (pos < current->nKeys && remain > 0) {
				result.push_back(current->pm_ptr->value[pos]);
				remain--;
				pos++;	
			}
			
			if (remain > 0 && current->RN != NULL) {
				a:
				current = current->RN;
				
				if (current->hot == true) {
					int i = 0;
					while (i < current->nKeys && remain > 0) {
						result.push_back(current->pm_ptr->value[i]);
						remain--;
						i++;
					}
				}
				
				else {
					key_value* value = (key_value*)aligned_alloc(PAGE_SIZE, sizeof(key_value));
					Read(value, current->zone_ptr, sizeof(key_value));
					int i = 0;
					while (i < current->nKeys && remain > 0) {
						result.push_back(value->value[i]);
						remain--;
						i++;
					}
					free(value);
				}
				if (remain > 0 && current->RN != NULL) {
					goto a;
				}

			}
			return result;
		}

		else {
			key_value* value = (key_value*)aligned_alloc(PAGE_SIZE, sizeof(key_value));
			Read(value, current->zone_ptr, sizeof(key_value));
			int pos = binarySearchForInsert(value->key, current->nKeys, s_key);
			while (pos < current->nKeys && remain > 0) {
				result.push_back(value->value[pos]);
				remain--;
				pos++;
			}
			
			if (remain > 0 && current->RN != NULL) {
				b:
				current = current->RN;
				
				if (current->hot == true) {
					int i = 0;
					while (i < current->nKeys && remain > 0) {
						result.push_back(current->pm_ptr->value[i]);
						remain--;
						i++;
					}
				}
				
				else {
					Read(value, current->zone_ptr, sizeof(key_value));
					int i = 0;
					while (i < current->nKeys && remain > 0) {
						result.push_back(value->value[i]);
						remain--;
						i++;
					}
					free(value);
				}
				if (remain > 0 && current->RN != NULL) {
					goto b;
				}

			}
			return result;
		
		}
	}


	void NormalRecovery() {
		firstleaf->parent = NULL;
		root = firstleaf;
		height = 1;
		leaf_node* current = firstleaf;
		while (current->RN != NULL) {
			insert_into_parent(current, current->RN->min_key, current->RN);
			current = current->RN;
		}

	}



	

};

