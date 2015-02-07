#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <iostream>
#include <stdlib.h>
#include <unistd.h>

using namespace std;

// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {

}

int DBFile::Create (char *f_path, fType f_type, void *startup) {
	
int open_status = file_obj.Open(0, f_path);

return open_status;
	
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
	
	
	FILE *TableFile = fopen(loadpath, "r");
	Record temp_rec;
	Page temp_page;
	int counter = 0;
	int page_no = 0;
	
	while(temp_rec.SuckNextRecord(&f_schema, TableFile) == 1) {
		
		
		counter++;
		
		if(counter % 10000 == 0) {
			
			cerr<<counter<<"\n";
			
				
		}
		temp_rec.Print(&f_schema);
		
		if(temp_page.Append(&temp_rec) == 0) {
			
			file_obj.AddPage(&temp_page,page_no);
			page_no++;
			temp_page.EmptyItOut();
			temp_page.Append(&temp_rec);			
			
			
			
		}
		
		
	}
	
	if(page_no == 0) {
		
		file_obj.AddPage(&temp_page,page_no);
		
	}
	

	//cout<<" no of pages = " << page_no<<"\n";
	
}

int DBFile::Open (char *f_path) {
	
	int open_status = file_obj.Open(1, f_path);
	return open_status;
	
}

void DBFile::MoveFirst () {
	
	lseek (file_obj.GetFilDes(), PAGE_SIZE, SEEK_SET);
	
}

int DBFile::Close () {
	
	int length = file_obj.Close();
	
	//cout<<"current length of file in pages ="<<length<<"\n";

	
}

void DBFile::Add (Record &rec) {
	
	
	off_t file_length = file_obj.GetLength();
	
	if(file_length>1) {
	
	file_obj.GetPage(&page_obj, file_length-2);
	
	if(page_obj.Append(&rec)==0) {
		
		
		page_obj.EmptyItOut();
		page_obj.Append(&rec);
		file_obj.AddPage(&page_obj,file_length-1);
		
	}
	
	} 
	else cerr<<"file is empty !!";
	
	
}

int DBFile::GetNext (Record &fetchme) {
	
	

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
	
	
	
	
	return 1;
	
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	
	
		ComparisonEngine comp;
	
		if(page_obj.GetNoOfRecords() == 0) {
	
	
			if((atPageNo+1) < file_obj.GetLength()) {
			
			file_obj.GetPage(&page_obj, atPageNo);
			atPageNo++;
			
		}
		else return 0;
		
	}

	
	while(1) {
	if(page_obj.GetFirst(&fetchme) == 0){
		
		if((atPageNo+1) < file_obj.GetLength()) {
			
			file_obj.GetPage(&page_obj, atPageNo);
			atPageNo++;
			
		}
		
		else return 0;
		
			
	}
	
	if (comp.Compare (&fetchme, &literal, &cnf))
                	return 1;
	

	
}
	
	
	return 1;
	
	
	
}
