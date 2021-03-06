#ifndef HEAP_DBFILE_H
#define HEAP_DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"
#include "GenericDBFile.h"



class HeapDBFile: public GenericDBFile{

private:
	File file_obj;
	Page page_obj;
	off_t atPageNo = 0;
	bool pageDirty = false;




public:
	HeapDBFile (); 

	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();

	void Load (Schema &myschema, char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);


	~HeapDBFile();
};

#endif
