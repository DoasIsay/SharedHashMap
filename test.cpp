#include "sharedHashMap.h"
class Test
{
	public:
		Test(){}
		int a;
		int b;
		Test(long i,long j):a(i),b(j){}
};

int main(int argvs,char *argv[])
{
	#ifdef _POSIX_THREAD_PROCESS_SHARED
	cout<<"yes"<<endl;
	#endif	
	typedef KVPair<int,Test> Node;
	cout<<sysconf(_SC_THREAD_PROCESS_SHARED)<<endl;
	HashTable<int,Test> map("MemoryPool",24,1024);

	Test a(22,2);
		
	
	int i = 0;
	for(;i<30000000;i++){
		if(argv[1][0]=='a'){
			map.set(i,a);
		}
		else if(argv[1][0]=='b'){
			map.get(i);
		}
		else if(argv[1][0]=='c'){
			map.del(i);
		}
	}
	cout<<i<<endl;
}
