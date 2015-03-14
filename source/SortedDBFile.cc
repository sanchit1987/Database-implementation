#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "SortedDBFile.h"
#include "Defs.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string>
#include <iostream>
#include <stdlib.h>
#include <unistd.h>
#include "Pipe.h"
#include "BigQ.h"
#include <fstream>
#include <cassert>
#include <cstdlib>




using namespace std;



SortedDBFile::SortedDBFile() {
	
	inPipe = new Pipe(100);	
	outPipe = new Pipe(100);	
	bq = NULL;
	sInfo = NULL;
	atPageNo = 0;
	
}


int SortedDBFile::Create (char *f_path, fType f_type, void *startup) {
	int open_status = file_obj.Open(0,f_path);

	sInfo = (SortInfo *)startup;
	char *fileHeaderName = new char[100];
	sprintf(fileHeaderName, "%s.header", f_path);
	FILE *f = fopen(fileHeaderName, "w");
	if(!f) {
		cerr << "Open file header name error!"<<endl;
		return 0;
	}
	fprintf(f, "%d\n", f_type);
	fprintf(f, "%d\n", sInfo->runLength);
	fprintf(f, "%s", sInfo->myOrder->ToString().c_str());
	cout<<"printing myorder by calling print function of OrderMaker inside create of SortedDBFile"<<endl;
	sInfo->myOrder->Print();
	cout<<"printing myorder inside create of SortedDBFile"<<endl<<sInfo->myOrder->ToString().c_str()<<endl;
	fclose(f);
	
	return open_status;


	
}

void SortedDBFile::Load (Schema &f_schema, char *loadpath) {
	
	cout<<"reached inside load of SortedDBFile" << endl;
	
	FILE *TableFile = fopen(loadpath, "r");
	Record temp_rec;
	Page temp_page;
	int page_no = 0;
	// OrderMaker ss = *(sInfo->myOrder);
	// int le = sInfo->runLength;
	Record temp;
	cout<<"printing myorder inside load of SortedDBFile"<<endl;
	sInfo->myOrder->Print();
	
	if(bq==NULL) bq = new BigQ(*inPipe,*outPipe,*(sInfo->myOrder),sInfo->runLength);


	
	while(temp_rec.SuckNextRecord(&f_schema, TableFile) == 1) {
		
		inPipe->Insert (&temp_rec);
		
	}

	cout<<"all the data has been pushed to inpipe for BigQ"<<endl;
	inPipe->ShutDown ();
	fclose(TableFile);
	
	
	while(outPipe->Remove(&temp_rec)) {
		temp_rec.Print(&f_schema);
		
	if(temp_page.Append(&temp_rec) == 0) {
			
			file_obj.AddPage(&temp_page,page_no);
			page_no++;
			temp_page.EmptyItOut();
			temp_page.Append(&temp_rec);			
			
			
			
		}
	}
	
		file_obj.AddPage(&temp_page,page_no);
		
	cout<<"end of load in SortedDBFile"<<endl;
	
	
	
}

int SortedDBFile::Open (char *f_path) {
	
	int open_status = file_obj.Open(1, f_path);
	sInfo = new SortInfo();
	int len;
	OrderMaker so;
	cout << "reached in open of SortedDBFile"<<endl;
	char *fileHeaderName = new char[100];
	sprintf(fileHeaderName, "%s.header", f_path);
	FILE *f = fopen(fileHeaderName,"r");
	int runlen;
	OrderMaker *o = new OrderMaker;
	fscanf(f, "%d", &runlen); //since the first line is the filetype
	fscanf(f, "%d", &runlen);
	sInfo->runLength = runlen;
	int attNum;
	fscanf(f, "%d", &attNum);
	o->numAtts = attNum;
	for(int i = 0;i<attNum;i++) {
	int att;
	int type;
	if(feof(f)) {
		cerr << "Retrieve ordermaker from file error"<<endl;
		return 0;
	}
	fscanf(f, "%d %d", &att, &type);
	o->whichAtts[i] = att;
	if(0 == type) {
		o->whichTypes[i] = Int;
	} else if(1==type) {
		o->whichTypes[i] = Double;
	} else
		o->whichTypes[i] = String;
	}
	sInfo->myOrder = o;
	

	cout<<"printing order inside open of SortedDBFile"<<endl;
	sInfo->myOrder->Print();
	return open_status;
	
	
}

void SortedDBFile::MoveFirst () {
	
	lseek (file_obj.GetFilDes(), PAGE_SIZE, SEEK_SET);
	atPageNo = 0;
	page_obj.EmptyItOut();
	

}

int SortedDBFile::Close () {
	
	int length = file_obj.Close();

	
}

void SortedDBFile::Add (Record &rec) {
	
	
	
	
		
	
}

int SortedDBFile::GetNext (Record &fetchme) {
	

	if(page_obj.GetNoOfRecords() == 0) {
	
	
		if(file_obj.GetPage(&page_obj, atPageNo) == 0)
				return 0;
				
				atPageNo++;
		
	}

	

	if(page_obj.GetFirst(&fetchme) == 0){
		
		
		
		
		if((atPageNo+1) < file_obj.GetLength()) {
			
			file_obj.GetPage(&page_obj, atPageNo);
			page_obj.GetFirst(&fetchme);
			
		}
		
		else return 0;
		
	}
	
	
	
	cout<<"reached end of GetNext of SortedDBFile"<<endl;
	return 1;
	

}

int SortedDBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	
ComparisonEngine comp;
	
		while(GetNext(fetchme)){
		if(comp.Compare (&fetchme, &literal, &cnf)==1)
			return 1;
	}

  return 0;

	
}

SortedDBFile::~SortedDBFile(){

}