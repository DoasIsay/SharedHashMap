#include<pthread.h>
#include<iostream>
#include <fcntl.h>  
#include <sys/stat.h> 
#include <semaphore.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include "sharedMem.h"
using namespace std;

typedef unsigned int unInt32;

class LockerOwner{
public:
	volatile int flag;
	#ifdef DEAD_LOCK_MONITOR
	unInt32 owner;
	#endif	
};

class AbstractLocker{
public:
	virtual int lock(unInt32 owner=0){;}
	virtual int ulock(unInt32 owner=0){;}
	virtual unInt32 getOwner(){return 0;}
	virtual void  setOwner(unInt32 owner){;}
	virtual ~AbstractLocker(){;}
	virtual bool tryLock(){;}
};
typedef AbstractLocker * AL;

class SpinLocker:public AbstractLocker{
private:
	LockerOwner *flagOwner;
public:
	SharedMem<LockerOwner> *shmFlagOwnerVar;
	SpinLocker(){
		flagOwner=new LockerOwner;
	}
	SpinLocker(LockerOwner *flagOwner){
		assert(flagOwner!=NULL);
		shmFlagOwnerVar=NULL;
		//__sync_val_compare_and_swap(&(flagOwner->flag),0,0);
		//#ifdef DEAD_LOCK_MONITOR
		//__sync_val_compare_and_swap(&(flagOwner->owner),0,0);
		//#endif
		this->flagOwner=flagOwner;
		//flagOwner->flag=flagOwner->owner=0;
	}
	SpinLocker(char *filePath=NULL){
		assert(filePath!=NULL);
		shmFlagOwnerVar=new SharedMem<LockerOwner>(filePath,8,true);
		this->flagOwner=(LockerOwner*)shmFlagOwnerVar->getPtr();
	}
	void *getLockerPtr(){
		return this->flagOwner;
	}
	int lock(unInt32 owner=0){
		while(__sync_lock_test_and_set(&(flagOwner->flag),1)){
			asm volatile("rep; nop");
		}
		if(owner!=0)
		setOwner(owner);
	}
	int ulock(unInt32 owner=0){
		//if((owner!=getOwner()))
		//	printf("warn the locker`s owner is %u, not %u\n",getOwner(),owner);
		setOwner(0);
		__sync_lock_release(&(flagOwner->flag));
	}
	bool tryLock(){
		return !__sync_lock_test_and_set(&(flagOwner->flag),1);
	}
	unInt32 getOwner(){
		#ifdef DEAD_LOCK_MONITOR
			return flagOwner->owner;
		#else
			return 0;
		#endif
	}
	void  setOwner(unInt32 owner){
		#ifdef DEAD_LOCK_MONITOR
			flagOwner->owner=owner;
		#endif
	}
	~SpinLocker(){
		#ifdef MULTI_THREAD
			if(flagOwner!=NULL) delete flagOwner;
		#endif
			if(shmFlagOwnerVar!=NULL) delete shmFlagOwnerVar;
	}
};

class Parker{
private:
	sem_t *sem;
	char filePath[256];
	volatile int waiters;
	SpinLocker *locker;
public:
	
	Parker(char *filePath){
		sem = sem_open(filePath, O_CREAT, 0555, 0);
		if(sem == NULL)
    		{
       			cout<<"sem_open  failed "<< filePath<<" "<<strerror(errno)<<endl;
       			sem_unlink(filePath);
        		exit(-1);
    		}
		
    		strcpy(this->filePath,filePath);
	}
	void park(){
		cout<<getpid()<<" park" <<endl;
		sem_wait(sem);	
	}
	void unpark(){
		sem_post(sem);
		cout<<getpid()<<" unpark"<<endl;
	}
	
	~Parker(){
		sem_close(sem);
    		sem_unlink(filePath);
	}
};

class SpinRwLock
{
private:
	Parker *parker;
	public:
		class Var{
		public:
			volatile int lock;
			volatile int wLock;
			volatile int reader;
			volatile int waiters;
		};
		Var *var;
		SharedMem<Var> *xxx;
		int loops;
		SpinRwLock(char *filePath)
		{
			xxx=new SharedMem<Var>(filePath,sizeof(Var),true);
			var=xxx->getPtr();
			loops=256;
			parker = new Parker("MemoryPool.park");
		}
		~SpinRwLock(){
			if(xxx!=NULL)
				delete xxx;
			if(parker!=NULL)
				delete parker;
		}
		void rlock()
		{	
			again:
			int count=0;
			while(__sync_lock_test_and_set(&(var->lock),1)){
				asm volatile("rep; nop");
				
			}
			if(var->reader==0)
			{
				while(__sync_lock_test_and_set(&(var->wLock),1)){
					asm volatile("rep; nop");
					count++;
					if(count<loops){
						continue;
					}else{
						__sync_lock_release(&(var->lock));
						var->waiters++;
						parker->park();
						if((--var->waiters)>0)
							parker->unpark();
						goto again;
					}
				}
			}
			++var->reader;
			
			__sync_lock_release(&(var->lock));
		}
		bool tryRlock()
		{	
			bool ret=false;			
			while(__sync_lock_test_and_set(&(var->lock),1)){
				asm volatile("rep; nop");
			}
			if(var->reader==0)
			{
				if(!__sync_lock_test_and_set(&(var->wLock),1)){
					ret=true;
					++var->reader;
				}
			}
			else{
				ret=true;
				++var->reader;
			}
			cout<<"reader "<<var->reader<<endl;
			__sync_lock_release(&(var->lock));
			return ret;
		}

		void wlock()
		{
			again:
			int count=0;
			while(__sync_lock_test_and_set(&(var->lock),1)){
				asm volatile("rep; nop");
				
			}
			while(__sync_lock_test_and_set(&(var->wLock),1)){
				asm volatile("rep; nop");
				count++;
				if(count<loops){
					continue;
				}else{
					
					count=0;
					var->waiters++;
					__sync_lock_release(&(var->lock));
					parker->park();
					if((--var->waiters)>0)
						parker->unpark();
					goto again;
				}
			}
			__sync_lock_release(&(var->lock));
		}

		bool tryWlock(){
			bool ret=false;
			while(__sync_lock_test_and_set(&(var->lock),1)){
			asm volatile("rep; nop");
			}
			if(var->reader==0){
				if(!__sync_lock_test_and_set(&(var->wLock),1))
					ret=true;
			}
			__sync_lock_release(&(var->lock));
			return ret;	
		}
		int runlock()
		{
			while(__sync_lock_test_and_set(&(var->lock),1)){
			asm volatile("rep; nop");
			}
			--var->reader;
			
			if(var->reader==0)
			{
				__sync_lock_release(&(var->wLock));
				__sync_synchronize();
				if(var->waiters>0)
				parker->unpark();
			}
			__sync_lock_release(&(var->lock));
			return 0;			
		}
		void wunlock()
		{
			__sync_lock_release(&(var->wLock));
			//__sync_synchronize();
			if(var->waiters>0)
			parker->unpark();		
		}
		void ulock()
		{
			__sync_synchronize();
			if(var->reader>0)
			{
				runlock();
			}
			else	
			{
				wunlock();
			}
		
		}
};

class AutoLocker{
private:
	AbstractLocker *locker;
public:
	
	AutoLocker(AbstractLocker *locker){
		this->locker=locker;
		locker->lock();
	}
	~AutoLocker(){
		locker->ulock();
	}
};
