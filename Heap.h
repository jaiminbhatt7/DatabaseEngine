#ifndef HEAP_H
#define HEAP_H

#include "GenericDBFile.h"
#include "Defs.h"
#include <vector>

//typedef enum {heap, sorted, tree} fType; //Indicates the type in which the files are stored

// stub DBFile header..replace it with your own DBFile.h

class Heap : public GenericDBFile
{
private:
	//Each DBFile instance has a "pointer" to the current record in the file.
	RecPointer CurrentRecord;
	//Whenever a new page is started, the older page is pushed to this vector
	vector<RecPointer> AllRecords;
	//A single instance of the File Class
	File CurrentFile;
	//A single instance of the page class
	Page CurrentPage;
	Page *Adder;
	//Something to check if dirty page has been added
	bool DirtyPageAdded;

public:
	Heap ();
	int Create (char *fpath, void *startup);
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
