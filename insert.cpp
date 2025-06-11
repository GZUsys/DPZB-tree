#include "DPZBtree.h"
std::vector<uint64_t> buffer;
std::vector<long> slen;
std::vector<char> ops;

#define test_num   3000000
#define load_num   3000000
#define LATENCY
#define bench_size 256
uint64_t records[50000000] = { 0 };
uint64_t latency, latency1, insert_nbs = 0;
struct timespec T1, T2;


void read_data_from_file(char* file)
{
	long count = 0;

	FILE* fp = fopen(file, "r");
	if (fp == NULL) {
		exit(-1);
	}
	buffer.clear();
	printf("reading\n");
	while (1) {
		unsigned long long key;
		count = fscanf(fp, "%lld\n", &key);
		if (count != 1) {
			break;
		}
		buffer.push_back(key);
	}
	fclose(fp);
	printf("file closed\n");
}


void scan_data_from_file(char* file)
{
	long count = 0;

	FILE* fp = fopen(file, "r");
	if (fp == NULL) {
		exit(-1);
	}
	buffer.clear();
	ops.clear();
	printf("reading\n");
	while (1) {
		char str[100];
		char* p;
		count = fscanf(fp, "%s\n", str);
		if (count != 1) {
			break;
		}
		p = strtok(str, ",");
		buffer.push_back(atoll(p));
		p = strtok(NULL, ",");
		ops.push_back(p[0]);
	}
	fclose(fp);
	printf("file closed\n");
}


void sd_data_from_file(char* file)
{
	long count = 0;

	FILE* fp = fopen(file, "r");
	if (fp == NULL) {
		exit(-1);
	}
	buffer.clear();
	slen.clear();
	printf("reading\n");
	while (1) {
		char str[100];
		char* p;
		count = fscanf(fp, "%s\n", str);
		if (count != 1) {
			break;
		}
		p = strtok(str, ",");
		buffer.push_back(atoll(p));
		p = strtok(NULL, ",");
		slen.push_back(atol(p));
	}
	fclose(fp);
	printf("file closed\n");
}


int main()
{
	
	char loading_file[100];
	sprintf(loading_file, "%s", "/root/lmj/datafile/loada.csv");
	read_data_from_file(loading_file);

	BPlusTree T;
	std::vector<uint64_t> buf;
	struct  timeval start_time, end_time;
	uint64_t time_interval;
	

	printf("loadb Loading...\n");
	gettimeofday(&start_time, NULL);
	for (int i = 0; i < load_num; i++) {
		buf.push_back(buffer[i]);
		if (buf.size() == bench_size) {
			clock_gettime(CLOCK_MONOTONIC, &T1);
			std::vector<uint64_t> hot_key;
			hot_key = KMeans(buf, 1, 2);
			for (int j = 0; j < hot_key.size(); j++) {
				T.hot_node_dispose(hot_key[j]);
			}
			clock_gettime(CLOCK_MONOTONIC, &T2);
			latency1 = ((T2.tv_sec - T1.tv_sec) * 1000000000 + (T2.tv_nsec - T1.tv_nsec)) / 100;

			
			for (int k = 0; k < bench_size; k++) {
				clock_gettime(CLOCK_MONOTONIC, &T1);
				T.insert(buf[k], buf[k]);
				clock_gettime(CLOCK_MONOTONIC, &T2);
				latency = ((T2.tv_sec - T1.tv_sec) * 1000000000 + (T2.tv_nsec - T1.tv_nsec)) / 100;
				if (k == 0) { latency = latency + latency1; }
				records[latency] += 1;
				insert_nbs += 1;
			}
			buf.clear();
		}
	}
	if (buf.empty() == false) {
		for (int i = 0; i < buf.size(); i++) {
			clock_gettime(CLOCK_MONOTONIC, &T1);
			T.insert(buf[i], buf[i]);
			clock_gettime(CLOCK_MONOTONIC, &T2);
			latency = ((T2.tv_sec - T1.tv_sec) * 1000000000 + (T2.tv_nsec - T1.tv_nsec)) / 100;
			records[latency] += 1;
			insert_nbs += 1;
		}
	}
	gettimeofday(&end_time, NULL);
	printf("loadb Loading complete.\n");
	time_interval = 1000000 * (end_time.tv_sec - start_time.tv_sec) + end_time.tv_usec - start_time.tv_usec;
	printf("load time = %lu ns\n", time_interval * 1000);


	uint64_t cnt = 0;
	uint64_t nb_min = insert_nbs * 0.1;
	uint64_t nb_50 = insert_nbs / 2;
	uint64_t nb_90 = insert_nbs * 0.9;
	uint64_t nb_99 = insert_nbs * 0.99;
	uint64_t nb_999 = insert_nbs * 0.999;
	uint64_t nb_9999 = insert_nbs * 0.9999;
	bool flag_50 = false, flag_90 = false, flag_99 = false, flag_min = false, flag_999 = false, flag_9999 = false;
	double latency_50, latency_90, latency_99, latency_min, latency_999, latency_9999;
	for (int i = 0; i < 50000000 && !(flag_min && flag_50 && flag_90 && flag_99 && flag_999 && flag_9999); i++) {
		cnt += records[i];
		if (!flag_min && cnt >= nb_min) {
			latency_min = (double)i / 10.0;
			flag_min = true;
		}
		if (!flag_50 && cnt >= nb_50) {
			latency_50 = (double)i / 10.0;
			flag_50 = true;
		}
		if (!flag_90 && cnt >= nb_90) {
			latency_90 = (double)i / 10.0;
			flag_90 = true;
		}
		if (!flag_99 && cnt >= nb_99) {
			latency_99 = (double)i / 10.0;
			flag_99 = true;
		}
		if (!flag_999 && cnt >= nb_999) {
			latency_999 = (double)i / 10.0;
			flag_999 = true;
		}
		if (!flag_9999 && cnt >= nb_9999) {
			latency_9999 = (double)i / 10.0;
			flag_9999 = true;
		}
	}
	printf("min latency is %.1lfus\nmedium latency is %.1lfus\n90%% latency is %.1lfus\n99%% latency is %.1lfus\n99.9%% latency is %.1lfus\n99.99%% latency is %.1lfus\n", latency_min, latency_50, latency_90, latency_99, latency_999, latency_9999);




	


	insert_nbs = 0;
	memset(records, 0, sizeof(records));  
	buffer.clear();
	buf.clear();

	sprintf(loading_file, "%s", "/root/lmj/datafile/insert3m.csv");
	read_data_from_file(loading_file);



	

	char op; op = 'i';
	
	switch (op) {
	case'i':
		gettimeofday(&start_time, NULL);
		for (int i = 0; i < test_num; i++) {
			buf.push_back(buffer[i]);
			if (buf.size() == bench_size) {
				clock_gettime(CLOCK_MONOTONIC, &T1);
				std::vector<uint64_t> hot_key;
				hot_key = KMeans(buf, 1, 2);
				for (int j = 0; j < hot_key.size(); j++) {
					T.hot_node_dispose(hot_key[j]);
				}
				clock_gettime(CLOCK_MONOTONIC, &T2);
				latency1 = ((T2.tv_sec - T1.tv_sec) * 1000000000 + (T2.tv_nsec - T1.tv_nsec)) / 100;

				
				for (int k = 0; k < bench_size; k++) {
					clock_gettime(CLOCK_MONOTONIC, &T1);
					T.insert(buf[k], buf[k]);
					clock_gettime(CLOCK_MONOTONIC, &T2);
					latency = ((T2.tv_sec - T1.tv_sec) * 1000000000 + (T2.tv_nsec - T1.tv_nsec)) / 100;
					if (k == 0) { latency = latency + latency1; }
					records[latency] += 1;
					insert_nbs += 1;
				}
				buf.clear();
			}
		}
		if (buf.empty() == false) {
			for (int i = 0; i < buf.size(); i++) {
				clock_gettime(CLOCK_MONOTONIC, &T1);
				T.insert(buf[i], buf[i]);
				clock_gettime(CLOCK_MONOTONIC, &T2);
				latency = ((T2.tv_sec - T1.tv_sec) * 1000000000 + (T2.tv_nsec - T1.tv_nsec)) / 100;
				records[latency] += 1;
				insert_nbs += 1;
			}
		}
		gettimeofday(&end_time, NULL);
		break;



	case 'r':
		gettimeofday(&start_time, NULL);
		

		for (int i = 0; i < test_num; i++) {
			buf.push_back(buffer[i]);
			if (buf.size() == bench_size) {
				clock_gettime(CLOCK_MONOTONIC, &T1);
				std::vector<uint64_t> hot_key;
				hot_key = KMeans(buf, 1, 2);
				for (int j = 0; j < hot_key.size(); j++) {
					T.hot_node_dispose(hot_key[j]);
				}
				clock_gettime(CLOCK_MONOTONIC, &T2);
				latency1 = ((T2.tv_sec - T1.tv_sec) * 1000000000 + (T2.tv_nsec - T1.tv_nsec)) / 100;

				for (int k = 0; k < bench_size; k++) {
					clock_gettime(CLOCK_MONOTONIC, &T1);
					T.search(buf[k]);
					clock_gettime(CLOCK_MONOTONIC, &T2);
					latency = ((T2.tv_sec - T1.tv_sec) * 1000000000 + (T2.tv_nsec - T1.tv_nsec)) / 100;
					if (k == 0) { latency = latency + latency1; }
					records[latency] += 1;
					insert_nbs += 1;
				}
				buf.clear();
			}
		}
		if (buf.empty() == false) {
			for (int i = 0; i < buf.size(); i++) {
				clock_gettime(CLOCK_MONOTONIC, &T1);
				T.search(buf[i]);;
				clock_gettime(CLOCK_MONOTONIC, &T2);
				latency = ((T2.tv_sec - T1.tv_sec) * 1000000000 + (T2.tv_nsec - T1.tv_nsec)) / 100;
				records[latency] += 1;
				insert_nbs += 1;
			}
		}
		gettimeofday(&end_time, NULL);

		break;


	case 'd':
		gettimeofday(&start_time, NULL);
		for (int i = 0; i < test_num; i++) {
			buf.push_back(buffer[i]);
			if (buf.size() == bench_size) {
				clock_gettime(CLOCK_MONOTONIC, &T1);
				std::vector<uint64_t> hot_key;
				hot_key = KMeans(buf, 1, 2);
				for (int j = 0; j < hot_key.size(); j++) {
					T.hot_node_dispose(hot_key[j]);
				}
				clock_gettime(CLOCK_MONOTONIC, &T2);
				latency1 = ((T2.tv_sec - T1.tv_sec) * 1000000000 + (T2.tv_nsec - T1.tv_nsec)) / 100;

				for (int k = 0; k < bench_size; k++) {
					clock_gettime(CLOCK_MONOTONIC, &T1);
					T.Delete(buf[k]);
					clock_gettime(CLOCK_MONOTONIC, &T2);
					latency = ((T2.tv_sec - T1.tv_sec) * 1000000000 + (T2.tv_nsec - T1.tv_nsec)) / 100;
					if (k == 0) { latency = latency + latency1; }
					records[latency] += 1;
					insert_nbs += 1;
				}
				buf.clear();
			}
		}
		if (buf.empty() == false) {
			for (int i = 0; i < buf.size(); i++) {
				clock_gettime(CLOCK_MONOTONIC, &T1);
				T.Delete(buf[i]);;
				clock_gettime(CLOCK_MONOTONIC, &T2);
				latency = ((T2.tv_sec - T1.tv_sec) * 1000000000 + (T2.tv_nsec - T1.tv_nsec)) / 100;
				records[latency] += 1;
				insert_nbs += 1;
			}
		}
		gettimeofday(&end_time, NULL);
		break;


	case 'u':
		gettimeofday(&start_time, NULL);
		for (int i = 0; i < test_num; i++) {
			buf.push_back(buffer[i]);
			if (buf.size() == bench_size) {
				clock_gettime(CLOCK_MONOTONIC, &T1);

				std::vector<uint64_t> hot_key;
				hot_key = KMeans(buf, 1, 2);
				for (int j = 0; j < hot_key.size(); j++) {
					T.hot_node_dispose(hot_key[j]);
				}

				clock_gettime(CLOCK_MONOTONIC, &T2);
				latency1 = ((T2.tv_sec - T1.tv_sec) * 1000000000 + (T2.tv_nsec - T1.tv_nsec)) / 100;

				for (int k = 0; k < bench_size; k++) {
					clock_gettime(CLOCK_MONOTONIC, &T1);
					T.Update(buf[k], buf[k]);
					clock_gettime(CLOCK_MONOTONIC, &T2);
					latency = ((T2.tv_sec - T1.tv_sec) * 1000000000 + (T2.tv_nsec - T1.tv_nsec)) / 100;
					if (k == 0) { latency = latency + latency1; }
					records[latency] += 1;
					insert_nbs += 1;
				}
				buf.clear();
			}
		}
		if (buf.empty() == false) {
			for (int i = 0; i < buf.size(); i++) {
				clock_gettime(CLOCK_MONOTONIC, &T1);
				T.Update(buf[i], buf[i]);
				clock_gettime(CLOCK_MONOTONIC, &T2);
				latency = ((T2.tv_sec - T1.tv_sec) * 1000000000 + (T2.tv_nsec - T1.tv_nsec)) / 100;
				records[latency] += 1;
				insert_nbs += 1;
			}
		}
		gettimeofday(&end_time, NULL);

		break;

	case 's':
		gettimeofday(&start_time, NULL);
		for (int i = 0; i < test_num; i++) {
			buf.push_back(buffer[i]);
			if (buf.size() == bench_size) {
				clock_gettime(CLOCK_MONOTONIC, &T1);

				std::vector<uint64_t> hot_key;
				hot_key = KMeans(buf, 1, 2);
				for (int j = 0; j < hot_key.size(); j++) {
					T.hot_node_dispose(hot_key[j]);
				}

				clock_gettime(CLOCK_MONOTONIC, &T2);
				latency1 = ((T2.tv_sec - T1.tv_sec) * 1000000000 + (T2.tv_nsec - T1.tv_nsec)) / 100;

				for (int k = 0; k < bench_size; k++) {
					clock_gettime(CLOCK_MONOTONIC, &T1);
					T.Scan(buf[k], 100);
					clock_gettime(CLOCK_MONOTONIC, &T2);
					latency = ((T2.tv_sec - T1.tv_sec) * 1000000000 + (T2.tv_nsec - T1.tv_nsec)) / 100;
					if (k == 0) { latency = latency + latency1; }
					records[latency] += 1;
					insert_nbs += 1;
				}
				buf.clear();
			}
		}
		if (buf.empty() == false) {
			for (int i = 0; i < buf.size(); i++) {
				clock_gettime(CLOCK_MONOTONIC, &T1);
				T.Scan(buf[i], 100);
				clock_gettime(CLOCK_MONOTONIC, &T2);
				latency = ((T2.tv_sec - T1.tv_sec) * 1000000000 + (T2.tv_nsec - T1.tv_nsec)) / 100;
				records[latency] += 1;
				insert_nbs += 1;
			}
		}
		gettimeofday(&end_time, NULL);

		break;



	default:
		printf("error\n");
		break;
	}



	time_interval = 1000000 * (end_time.tv_sec - start_time.tv_sec) + end_time.tv_usec - start_time.tv_usec;
	printf("All time_interval = %lu ns\n", time_interval * 1000);


	cnt = 0;
	nb_min = insert_nbs * 0.1;
	nb_50 = insert_nbs / 2;
	nb_90 = insert_nbs * 0.9;
	nb_99 = insert_nbs * 0.99;
	nb_999 = insert_nbs * 0.999;
	nb_9999 = insert_nbs * 0.9999;
	flag_50 = false, flag_90 = false, flag_99 = false, flag_min = false, flag_999 = false, flag_9999 = false;
	latency_50 = 0, latency_90 = 0, latency_99 = 0, latency_min = 0, latency_999 = 0, latency_9999 = 0;
	for (int i = 0; i < 50000000 && !(flag_min && flag_50 && flag_90 && flag_99 && flag_999 && flag_9999); i++) {
		cnt += records[i];
		if (!flag_min && cnt >= nb_min) {
			latency_min = (double)i / 10.0;
			flag_min = true;
		}
		if (!flag_50 && cnt >= nb_50) {
			latency_50 = (double)i / 10.0;
			flag_50 = true;
		}
		if (!flag_90 && cnt >= nb_90) {
			latency_90 = (double)i / 10.0;
			flag_90 = true;
		}
		if (!flag_99 && cnt >= nb_99) {
			latency_99 = (double)i / 10.0;
			flag_99 = true;
		}
		if (!flag_999 && cnt >= nb_999) {
			latency_999 = (double)i / 10.0;
			flag_999 = true;
		}
		if (!flag_9999 && cnt >= nb_9999) {
			latency_9999 = (double)i / 10.0;
			flag_9999 = true;
		}
	}
	printf("min latency is %.1lfus\nmedium latency is %.1lfus\n90%% latency is %.1lfus\n99%% latency is %.1lfus\n99.9%% latency is %.1lfus\n99.99%% latency is %.1lfus\n", latency_min, latency_50, latency_90, latency_99, latency_999, latency_9999);

	

	return 0;

}