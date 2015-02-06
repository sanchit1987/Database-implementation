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
	
	 
	 if(f_type == heap) {
	 
	 mode = O_TRUNC | O_RDWR | O_CREAT;
	 
	 myFilDes = open (f_path, mode, S_IRUSR | S_IWUSR);
	 
	 if (myFilDes < 0) {
		return 0;
	}
	else return 1;
}
	
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
}

int DBFile::Open (char *f_path) {
}

void DBFile::MoveFirst () {
}

int DBFile::Close () {
}

void DBFile::Add (Record &rec) {
}

int DBFile::GetNext (Record &fetchme) {
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
}
