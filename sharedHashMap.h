#include<iostream>
#include "allocator.h"
using namespace std;

class HashInt
{
public:
	long operator()(const long &key,const long &mask) {return key%mask;}
};

class EquleInt
{
public:
	bool operator()(const long &key1,const long &key2) {return key1==key2;}
};

template<class KeyType,class ValueType>
class KVPair
{
public:
	KeyType   key;
	ValueType value;
	unsigned int next;
};

typedef unsigned int unInt32;

template<class KeyType,class ValueType,class Hash=HashInt,class Equle=EquleInt>
class HashTable
{
private:
	typedef KVPair<KeyType,ValueType> Node;
	class Slot{
	public:
		unsigned int next;
		unsigned int lock;
		unsigned int changes;
	};
	
	class MetaData{
	public:
		unsigned int curMovingIdx;
		int rehashing;
		int status;
		unsigned int slots;
		int expandId;
		int rehashId;
		char curUsingSlotFile[256];
		char curExingSlotFile[256];
		int slotUsingRef;
		int rehashDone;
		int runnings;
	};
	AL *slotLockerA,*slotLockerB;
	MetaData *metaData;
	long maskA,cap,maskB;
	long slotsA,slotsB;
	SharedMem<MetaData> *metaDataMem;
	SharedMem<Slot> *slotMemA,*slotMemB;
	Slot *slotA,*slotB;
	Equle equle;
	Hash hash;
	Allocator<Node> *nodeAlloc;
	Allocator<LockerOwner> *lockAlloc;
	AbstractLocker *rehashLocker;
	int localExpandId,localRehashId;
	char filePath[256];
	char slotFilePath[256];
	char nodeFilePath[256];
	char lockFilePath[256];
	char uniqLockFilePath[256];
	char metaDataFilePath[256];
	SpinRwLock *locker;
	void initSlot(Slot *slot,AL *slotLocker,int size){
		__sync_synchronize();
		for(long i=0;i<size;i++){
			
			if(!metaData->status){//maybe another process is already inited
		
				unsigned int lock=lockAlloc->alloc();
				LockerOwner *lo=lockAlloc->getObj(lock);
				SpinLocker* locker=new SpinLocker(lo);			
				lo->flag=0;
				slotLocker[i]=locker;
				slot[i].lock=lock;
				slot[i].changes=0;
			}else{
				LockerOwner *lo=lockAlloc->getObj(slot[i].lock);
				SpinLocker* locker=new SpinLocker(lo);			
				slotLocker[i]=locker;
			}
		}
		lockAlloc->setStatus(2);
		__sync_synchronize();
	}
public:
	HashTable()
	{
		slotMemA=NULL;
		slotA=NULL;
		maskA=0;
		nodeAlloc=NULL;
		lockAlloc=NULL;
	}
	HashTable(char *filePath,long slots,long cap)
	{
		slotMemA=NULL;
		slotA=NULL;
		this->slotsA=slots;
		this->cap=cap;
		strcpy(this->filePath,filePath);
		init(filePath,slots,cap);
		
	}
	HashTable(const HashTable &hashTable)=delete;
	
	void init(char *filePath,long slots,long cap){
		strcpy(metaDataFilePath,filePath);
		strcat(metaDataFilePath,"Meta");
		metaDataMem=new SharedMem<MetaData>(metaDataFilePath,sizeof(MetaData));
		metaData=metaDataMem->getPtr();

		__sync_fetch_and_add(&(metaData->runnings),1);

		strncpy(uniqLockFilePath,filePath,256);
		strcat(uniqLockFilePath,".lock");
		locker=new SpinRwLock(uniqLockFilePath);

		strcpy(nodeFilePath,filePath);
		strcat(nodeFilePath,"Nodes");
		nodeAlloc=new Allocator<Node> (nodeFilePath,cap);
		
		strcpy(lockFilePath,filePath);
		strcat(lockFilePath,"Locks");
		lockAlloc=new Allocator<LockerOwner> (lockFilePath,50000000);
		locker->wlock();
		
		if(metaData->curUsingSlotFile[0]==0){
			char tmpFile[256];
			strcpy(tmpFile,filePath);
			strcat(tmpFile,"SlotsA");
			strcpy(metaData->curUsingSlotFile,tmpFile);
			strcpy(tmpFile,filePath);
			strcat(tmpFile,"SlotsB");
			strcpy(metaData->curExingSlotFile,tmpFile);
		}
		
		if(metaData->slots==0)
		metaData->slots=this->slotsA=(slots<2?2:slots);
		cout<<"metaDataSlots "<<metaData->slots<<endl;

		localExpandId=metaData->expandId;
		
		if(metaData->rehashing){
			maskA=this->slotsA=metaData->slots/2;
		}else if(metaData->rehashDone){
			maskA=this->slotsA=metaData->slots/2;
			metaData->rehashing=1;
			metaData->rehashDone=0;
		}
		else{
			this->slotsA=maskA=metaData->slots;
		}
		maskB=0;
		slotMemB=NULL;
		slotB=NULL;
		cout<<"cur use file "<<metaData->curUsingSlotFile<<" slots "<<this->slotsA<< endl;
		cout<<"cur exp file "<<metaData->curExingSlotFile<<" slots "<<this->slotsA<< endl;
		slotMemA=new SharedMem<Slot>(metaData->curUsingSlotFile,this->slotsA*sizeof(Slot));
		slotA=slotMemA->getPtr();
		slotLockerA=new AL[this->slotsA];

		if(slotA!=NULL)
		initSlot(slotA,slotLockerA,this->slotsA);

		metaData->status=1;
		cout<<"init slots "<<slotsA<<endl;
		locker->ulock();
	}
	Node operator[](const KeyType &key){
		return get(key);
	}
	
	~HashTable(){
		__sync_fetch_and_sub(&(metaData->runnings),1);
		if(metaData->runnings==0){
			metaData->status=0;
			metaData->rehashDone=0;
		}
		if(nodeAlloc!=NULL) delete nodeAlloc;
		if(lockAlloc!=NULL) delete lockAlloc;
		if(slotMemA!=NULL)   delete slotMemA;
		if(locker!=NULL)    delete locker;
	}
	bool destoryFlag;

	int move(unsigned int key,unsigned int &head){
		int idx=hash(key,maskA);
		unsigned int next=slotA[idx].next;
		if(next==0)
			return 0;
		Node *tmp=nodeAlloc->getObj(next);
		if(equle(tmp->key,key)){
			slotA[idx].next=tmp->next;
			insertNode(next,head);
			slotA[idx].changes++;
			return 0;
		}
		unsigned int pre=next;
		next=tmp->next;
		while(next!=0){
			tmp=nodeAlloc->getObj(next);
			cout<<tmp->key<<endl;
			
			if(equle(tmp->key,key)){
				nodeAlloc->getObj(pre)->next=tmp->next;
				insertNode(next,head);
				slotA[idx].changes++;
				return 0;
			}
			pre=next;
			next=tmp->next;
		}
		return -1;		
	}

	void doRehash(){
		cout<<"metaData->curMovingIdx "<<metaData->curMovingIdx<<endl;
		__sync_fetch_and_add(&(metaData->rehashDone),1);
		loop:
		int idx=__sync_fetch_and_add(&(metaData->curMovingIdx),1);
		if(idx==slotsA){
			__sync_fetch_and_sub(&(metaData->rehashDone),1);
			rehashOver();
			return;
		}
		else if(idx>slotsA){
			__sync_fetch_and_sub(&(metaData->rehashDone),1);
			destorySlotA();
			switchTo();
			return;
		}
		else if(metaData->rehashing==1){
			//cout<<"slotsA "<<slotsA<<" curMovingIdx "<<idx<<endl;
			slotLockerA[idx]->lock();
			unsigned int next=slotA[idx].next;
			for(;next!=0;){
				Node *node=nodeAlloc->getObj(next);
				next=node->next;
				int idx_1=hash(node->key,maskB);
				slotLockerB[idx_1]->lock();
				move(node->key,slotB[idx_1].next);
				
				slotLockerB[idx_1]->ulock();
			}
			slotLockerA[idx]->ulock();
		}
		else{
			__sync_fetch_and_sub(&(metaData->rehashDone),1);
			destorySlotA();
			switchTo();
			return;
		}
		goto loop;
	}
	void waitForRehashDone(){
		__sync_synchronize();
		while(metaData->rehashDone){ 
			cout<<"wait other over "<<metaData->rehashDone<<endl;
			//checkAliveProcess();
			sleep(1);
			__sync_synchronize();
		}
	}
	void rehashOver(){
		cout<<"rehash over "<<metaData->curMovingIdx<<endl;		
		
		__sync_synchronize();
		waitForRehashDone();
		destoryLocker();
		destorySlotA();
		switchTo();
		metaData->rehashing=0;
		metaData->curMovingIdx=0;
		__sync_synchronize();
		char tmpFile[256];
		strcpy(tmpFile,metaData->curUsingSlotFile);
		strcpy(metaData->curUsingSlotFile,metaData->curExingSlotFile);
		strcpy(metaData->curExingSlotFile,tmpFile);	
		
	}
	void switchTo(){
		if(slotB!=NULL&&slotMemB!=NULL){
			cout<<"destory slotA switch to slotB "<<endl;	
			slotMemA=slotMemB;
			slotA=slotB;
			slotLockerA=slotLockerB;
			slotB=NULL;
			slotMemB=NULL;	
			slotLockerB=NULL;
		}
		else{
			printf("switch fail slotB is not available\n");
			exit(-1);
		}
		maskA=slotsA=slotsB;
		maskB=0;
		slotsB=0;
		
		
	}
	void destoryLocker(){
		for(int idx=0;idx<slotsA;++idx){
			if(slotA[idx].lock!=0){
				if(metaData->rehashDone==0){
					lockAlloc->free(slotA[idx].lock);
					slotA[idx].lock=0;
				}
				
			}
		}
	}

	void destorySlotA(){
		for(int idx=0;idx<slotsA;++idx){
			delete slotLockerA[idx];
		}
		if(slotA!=NULL){
			slotA=NULL;
		}
		if(slotMemA!=NULL){
			delete slotMemA;
			slotMemA=NULL;
		}
		if(slotLockerA!=NULL){
			delete slotLockerA;
			slotLockerA=NULL;
		}
	}

	unsigned int size(){return nodeAlloc->used();}

	void doExpand(char *fielPath,int size){
		slotsB=maskB=size;
		slotMemB=new SharedMem<Slot>(fielPath,size*sizeof(Slot));
		slotB=slotMemB->getPtr();
		slotLockerB=new AL[size];
		initSlot(slotB,slotLockerB,size);
	}
	
	int expand(char *filePath,int size){
		doExpand(filePath,size);
	}
	
	int rehash(){
		if((size()>metaData->slots)&&(metaData->slots<1000000000)&&(metaData->rehashDone==0)){//declare a expand
			cout<<"rehash expandId begin "<<localExpandId<<" expand size "<<size()<<" metadata->slots "<< metaData->slots<<" curExingFile "<<metaData->curExingSlotFile<<endl;
			locker->wlock();
			if((size()<metaData->slots)||(metaData->slots>1000000000)||metaData->rehashDone!=0){
				cout<<"rehash already done"<<endl;
				locker->ulock();
				return 0;
			}
			
			unsigned int size=metaData->slots*2;
			metaData->status=0;
			expand(metaData->curExingSlotFile,size);
			localExpandId=metaData->expandId+1;
			metaData->slots=size;
			metaData->status=1;
			metaData->rehashing=1;
			metaData->curMovingIdx=0;
			metaData->rehashDone=0;
			metaData->expandId = localExpandId;
			locker->ulock();

			locker->rlock();
			doRehash();
			cout<<"rehash expandId end "<<localExpandId<<" expand size "<<size<<" metadata->slots "<< metaData->slots<<" curExingFile "<<metaData->curExingSlotFile<<endl;
			locker->ulock();	
		}
	}
	int checkRehash(){
		if((metaData->rehashing==1||localExpandId!=metaData->expandId)&&slotsA!=metaData->slots){//declare a expand
			cout<<"check rehash expandId begin "<<localExpandId<<" expand size "<<size()<<" metadata->slots "<< metaData->slots<<" curExingFile "<<metaData->curExingSlotFile<<endl;
			
			locker->rlock();
			expand(metaData->curExingSlotFile,metaData->slots);
			if(metaData->rehashDone==0){ 
				locker->ulock();
				localExpandId=metaData->expandId;
				cout<<"check rehash already done"<<endl;
				return 0;
			}
			
			doRehash();
			
			localExpandId=metaData->expandId;
			cout<<"check rehash expandId end "<<localExpandId<<" expand size "<<size()<<" metadata->slots "<< metaData->slots<<" curExingFile "<<metaData->curExingSlotFile<<endl;
			locker->ulock();
		}
	}
	Node *set(const KeyType &key,const ValueType &value){
		locker->rlock();
		checkRehash();
		Slot *tmpSlot=slotA;
		AL *tmpSlotLocker=slotLockerA;
		int idx=hash(key,maskA);
		tmpSlotLocker[idx]->lock();

		
		Node *node=findNode(key,tmpSlot[idx].next);
	
		if(node==NULL){
			unsigned int offset=nodeAlloc->alloc();
			if(offset==0){
				locker->ulock();
				tmpSlotLocker[idx]->ulock();
				return NULL;
			}
			node=nodeAlloc->getObj(offset);                              
			if(node==NULL){
				locker->ulock();
				tmpSlotLocker[idx]->ulock();
			        return NULL; 
			}
			                            
			node->key=key;                                
			node->value=value;
			insertNode(offset,tmpSlot[idx].next);
			slotA[idx].changes++;
			locker->ulock();
			tmpSlotLocker[idx]->ulock();
			rehash();
			return node;        
		}                                       
		node->value=value;
		slotA[idx].changes++;
		locker->ulock();
		tmpSlotLocker[idx]->ulock();
		return node;                                            
	}

	Node get(const KeyType &key){
		again:
		checkRehash();
		int idx=hash(key,maskA);

		unsigned int next=slotA[idx].next;
		Node *tmp=nodeAlloc->getObj(next);
		unsigned int changes=slotA[idx].changes;
	
		while(!equle(tmp->key,key))
		{	
			if(changes!=slotA[idx].changes){
				cout<<"changs "<<endl;
				goto again;

			}
			//cout<<"pass "<<tmp->key<<endl;
			if(!tmp->next)
				return Node();
			tmp=nodeAlloc->getObj(tmp->next);
			
		}
		if(changes!=slotA[idx].changes){
			cout<<"changs "<<endl;
			goto again;
		}
		//cout<<"get "<<tmp->key<<endl;
		return *tmp;
	}
	int del(const KeyType &key){
		locker->rlock();
		checkRehash();
		int idx=hash(key,maskA);
		AutoLocker al(slotLockerA[idx]); 
		unsigned int pre=0;
		unsigned int next=slotA[idx].next;
		Node *tmp=NULL;
		if(equle(nodeAlloc->getObj(next)->key,key)){
			nodeAlloc->free(next);
			slotA[idx].next=0;
			slotA[idx].changes++;
			goto ret;
		}
		
		pre=next=nodeAlloc->getObj(next)->next;
		
		while(next!=0){
			unsigned int tmpNextNext=nodeAlloc->getObj(next)->next;
			if(equle(nodeAlloc->getObj(next)->key,key)){
				nodeAlloc->getObj(pre)->next=tmpNextNext;
				nodeAlloc->free(next);
				slotA[idx].changes++;
				goto ret;
			}
			
			pre=next;
			next=tmpNextNext;
		}
		ret:
		locker->ulock();
		return -1;			
	}
private:
	Node *findNode(const KeyType &key,unsigned int next){	
		if(next==0)
		return NULL;
		Node *tmp=nodeAlloc->getObj(next);
		while(!equle(tmp->key,key))
		{
			//cout<<"pass "<<tmp->key<<endl;
			if(!tmp->next)
				return NULL;
			tmp=nodeAlloc->getObj(tmp->next);
		}
		return tmp;
	}
	
	int  insertNode(unsigned int offset,unsigned int &next){
		if(offset!=0)
		{
			if(next!=0)
			{
				nodeAlloc->getObj(offset)->next=next;
			}
			else
			{
				nodeAlloc->getObj(offset)->next=0;
			}
			next=offset;
			return 0;
		}
		return -1;
	}
};
