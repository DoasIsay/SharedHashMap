#include "locker.h"

template<class Type>
class Allocator
{        
private:
	class MetaData{
	public:
		int status;//0:not inialized 1:inialized 2:alloced 3:be expanding
		unsigned int used;
		unsigned int cap;
		unsigned int freeList;
		int expandId;
	};
	Type *head;
	MetaData *metaData;
	int localExpandId;
	AbstractLocker *locker;
	SharedMem<Type> *memPool;
	char lockerFilePath[256];
	
public:
	int getExpandId(){ return metaData->expandId;}
	char* getHead(){
		if(localExpandId==metaData->expandId)
			return (char*)head;
		else{//other process has already expanded,now mremap
			cout<<"alloc local expandid "<<localExpandId<<" expandid "<<metaData->expandId<<endl;
			memPool->remap();
			initMem(0,0);
			localExpandId=metaData->expandId;
		}
		return (char*)head;
		
	}
	unsigned int getOffset(Type *ptr){ (char*)ptr-getHead();}
	Type *getObj(unsigned int offset){ return (Type*)(getHead()+offset);}
	Allocator(char *filePath,long cap,bool delFlag=false)
	{
		#ifdef MUTEX_LOCKER
		#else
		strcpy(lockerFilePath,filePath);
		strcat(lockerFilePath,".lock");
		cout<<lockerFilePath<<endl;
		locker=new SpinLocker(lockerFilePath);
		#endif	
		init(filePath,cap,delFlag);
	}
	void initMem(long cap,unsigned int memStart){
		printf("init mem cap %d memStart %d\n",cap,memStart);
		Type *ptr;
		head=(Type *)memPool->getPtr();
		metaData=(MetaData*)head;

		ptr=(Type*)((char*)head+memStart+sizeof(MetaData));

		unsigned int tmpCap=cap-1;
		cout<<"used "<<metaData->used<<endl;
		__sync_synchronize();
		if(getStatus()!=0){//already inialized
			if(memStart==0)//first inialize
				return;
			else{//expand
				tmpCap=metaData->cap-1;
				goto lable;
			}
		}
		cout<<"status "<<getStatus()<<endl;
		metaData->used=0;
		metaData->cap=cap;
		localExpandId=metaData->expandId=0;
		setStatus(1);
		lable:
		metaData->freeList=(unsigned int)ptr-(unsigned int)head;
		int idx=0;
		cout<<"sizeof Type "<<sizeof(Type)<<endl;
		cout<<"cap "<<tmpCap<<" "<<endl;
		for(;idx<tmpCap;++idx)
		{
			*(unsigned int*)(ptr+idx)=(unsigned int)(ptr+idx+1)-(unsigned int)head;//relate offset
		}
		*(unsigned int*)(ptr+idx)=0;
		unsigned int start=metaData->freeList;
		for(int i=0;i<tmpCap;++i)
		{
			if(start==0) break;
			start=*(unsigned int*)((char*)head+start);
		}
		
		
		
	}
	void init(char *filePath,long cap,bool delFlag)
	{
		AutoLocker al(locker); 
		memPool=new SharedMem<Type>(filePath,sizeof(Type)*cap+sizeof(MetaData),delFlag);
		initMem(cap,0);
	}
	
	int expandMem(){
		unsigned int oldCap=metaData->cap;
		unsigned int cap=oldCap*2;
		unsigned int memStart=metaData->cap*sizeof(Type);
		unsigned int newSize=cap*sizeof(Type)+sizeof(MetaData);
		if(memPool->expand(newSize)>=0)
		printf("expand sucessfully\n");
		initMem(oldCap,memStart);
		metaData->cap=cap;
		localExpandId=metaData->expandId+=1;
	}
	
	
public:
	int getStatus() { //0:not inialize; 1:inialized but not used; 2:already inialiezd and being used;
		return metaData->status;
	}
	int setStatus(int status){ return metaData->status=status;}	
	unsigned int free() { return metaData->cap-metaData->used; }
	unsigned int used() { return metaData->used;}
	unsigned int capacity()  { return metaData->cap; }
	unsigned int alloc()
	{
		AutoLocker al(locker);

		again:
		char *head=getHead();
		unsigned int offset=metaData->freeList;
		if(offset!=0){	
			metaData->freeList=*(unsigned int*)(head+offset);
			++(metaData->used);
			return offset;
		}
		else{
			
			expandMem();
			goto again;
		}
	}

	unsigned int free(unsigned int offset)
	{
		AutoLocker al(locker);
		char *head=getHead();
		*(unsigned int*)(head+offset)=metaData->freeList;
		metaData->freeList=offset;
		--(metaData->used);
		return 0;
	}
	
	~Allocator()
	{
		if(locker!=NULL) delete locker;
		if(memPool!=NULL) delete memPool;
	}	
};
