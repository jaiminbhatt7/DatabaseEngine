#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Heap.h"
#include "Sort.h"
#include "GenericDBFile.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
//#include <vector>
#include <string>
#include <fstream>

typedef enum {heap, sorted, tree} fType; //Indicates the type in which the files are stored

// stub DBFile header..replace it with your own DBFile.h

class DBFile {
private:


	//A single instance of the File Class

	//A single instance of the page class

public:
	ofstream metafile;
	ifstream readmetafile;
	int runlength;
	string type;	//sort heap tree
	OrderMaker *SortOrder;	//for sorted file
	GenericDBFile *myInternalVar;
	DBFile ();
	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();

	void Load (Schema &myschema, char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
	void AddPage();
};
#endif
