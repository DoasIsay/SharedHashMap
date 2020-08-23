#include<iostream>
#include<time.h>
#include<memory.h>
#include<stdlib.h>
#include<iostream>
#include<sys/types.h>
#include<stdio.h>
#include <unistd.h>
#include<fcntl.h>
#include<sys/mman.h>
#include<sys/stat.h>
#include<stdlib.h>
#include<string.h>
#include<assert.h>
#include<sys/time.h>
#include<pthread.h>
using namespace std;

template<class Type>
class SharedMem{
private:
	Type *obj;
	int *ref;
	void *mem;
	int *size;
	int localSize;
	char filePath[256];
	bool delFlag;
	int fd;
public:

	SharedMem(char *filePath,int size=sizeof(Type),bool delFlag=false){
		int tmpSize;
		this->delFlag=delFlag;
		tmpSize=size+sizeof(int)*2;
		strncpy(this->filePath,filePath,256);
		fd=open(filePath,O_RDWR|O_CREAT);
		if(fd==-1){
			printf("open file %s error:%m\n",filePath);
			exit(-1);
		}
		struct stat status;
		if(fstat(fd,&status)<0){
			exit(-1);
		}
		if(status.st_size<tmpSize){
			if(fallocate(fd,0,0,tmpSize)<0){
			printf("stat file %s error:%m\n",filePath);
				printf("fallocate file %s error:%m\n",filePath);
				exit(-1);
			}
			cout<<"set is 0"<<endl;
		}
		else
		tmpSize=status.st_size;
		mem=mmap(NULL,tmpSize,PROT_READ|PROT_WRITE,MAP_SHARED,fd,0);
		if(mem==NULL){
			printf("mmap file %s error:%m\n",filePath);
			exit(-1);
		}

		ref=(int*)mem;
		this->size=(int*)((char*)mem+sizeof(int));
		localSize=*(this->size)=tmpSize;
		obj=(Type*)((char*)mem+sizeof(int)*2);
		incRef();
	}

	int incRef(){	return __sync_add_and_fetch(ref,1); }
	int decRef(){	return __sync_sub_and_fetch(ref,1); }

	void set(Type value,int index=0){
		*(obj+index)=value;
	}

	Type get(int index=0){
		return *(obj+index);
	}
 
	Type operator [](int index){
		return get(index);
	}

	Type *getPtr(int index=0){ return obj+index;}


	int remap(int oldSize,int newSize=1024*1024*1024){
		if(ftruncate(fd,newSize)<0){
			printf("expand %s fial:%m\n",filePath);
			return -1;
		}
		cout<<"start old "<<oldSize<<" new "<<newSize<<endl;;
		void *tmp=mremap(mem,oldSize,newSize,MREMAP_MAYMOVE);
		cout<<"end";
		if(tmp!=NULL){
			mem=tmp;
			ref=(int*)mem;
			size=(int*)((char*)mem+sizeof(int));
			
			obj=(Type*)((char*)mem+sizeof(int)*2);
			return 0;
		}
		else{
			printf("mremap %s fail:%m\n",filePath);
			return -1;
		}
	}
	int remap(){
		remap(localSize,*size);
		return localSize=*size;
	}
	int expand(int newSize){
		int tmpSize=newSize+sizeof(int)*2;
		remap(localSize,tmpSize);
		localSize=*size=tmpSize;
	}
	~SharedMem(){
		printf("munmap ref %d %s \n",*ref,filePath);
		if((decRef()==0)&&delFlag&&(access(filePath,F_OK)==0))//when multi process exit concurrently ,maybe remove failed,but it does`t matter,we just ignore it
		{
			printf("ref %d remove %s\n",*ref,filePath);
			munmap(mem,localSize);
			remove(filePath);
		}
		else
			munmap(mem,localSize);
			cout<<"end"<<endl;
		
	}
};
