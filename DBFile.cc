#include "DBFile.h"
#include <assert.h>
#include <iostream>
#include <stdlib.h>



extern "C"{
int yyparse(void);
}

extern struct AndList *final;
struct SortInfo {
	OrderMaker *myOrder;
	int runLength;
} *sortedInfo;

DBFile::DBFile () {

}

int DBFile::Create (char *f_path, fType f_type, void *startup) {
	//create an entry in the meta file
	//*startup is a dummy parameter not used in Assignment 1

	//cout<<" create " << f_path;
	string chkstr = f_path;
	int end = chkstr.length()-1;
	int pos=0;
	//cout<<"Before doing chkstr stuff"<<chkstr<<endl;
	while(chkstr.length()>0)
	{
		pos = chkstr.find("/",2);
		//cout<<"Position"<<pos<<endl;
		if(pos <= 0)
			break;
		chkstr = chkstr.substr(pos, chkstr.length());
	}
	chkstr = chkstr.substr(1,chkstr.length());
	//cout<<"After chkstr stuff"<<chkstr<<endl;
	chkstr.append("metafile");

	metafile.open(chkstr.c_str());
	//cout<<" created ";
	if(metafile.good())
	{
		if(f_type==0)			//heap
		{
			myInternalVar = new Heap();
			metafile<<f_path<<endl;
			metafile<<"0";		//write 0 into the file
		}
		else if(f_type==1)		//sorted
		{
			myInternalVar = new Sort();
			metafile<<f_path<<"\n";
			metafile<<"1";

			sortedInfo = (SortInfo*) startup; //pointer to SortInfo
			metafile<<"\n"<<sortedInfo->runLength;
			metafile<<"\n"<<sortedInfo->myOrder->numAtts<<"\n";
			for(int i=0; i<sortedInfo->myOrder->numAtts;i++){
				metafile<<sortedInfo->myOrder->whichAtts[i]<<"\n";
			}
			metafile<<";\n";
			for(int i=0; i<sortedInfo->myOrder->numAtts;i++){
				metafile<<sortedInfo->myOrder->whichTypes[i]<<"\n";
			}
			cout<<" write to file done ";
		}
		else if(f_type==2)		//tree
		{
			metafile<<"2";
		}
		metafile.close();
	}
	else{
		//	cout<<"Default creating heap file"<<endl;
		myInternalVar = new Heap();
	}

	myInternalVar->Create(f_path, sortedInfo);
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
	//Bulk loads the DBFile instance from a text file, appending new data to it using the SuckNextRecord function
	//loadpath is the name of the data file to bulk load
	myInternalVar->Load( f_schema,  loadpath);
}

int DBFile::Open (char *f_path) {

	//Function assumes that the DBFile has been created and closed before.
	//If any other parameters are needed, they should be written into the auxillary .header file before which should also be opened at startup
	//Returns 1 on success and 0 on failure

	// FILE * pFile;
	string chkstr = f_path;
	int end = chkstr.length()-1;
	int pos=0;
	while(chkstr.length()>0)
	{
		pos = chkstr.find("/",2);
		if(pos <= 0)
			break;
		chkstr = chkstr.substr(pos, chkstr.length());
	}
	chkstr = chkstr.substr(1,chkstr.length());
	chkstr.append("metafile");
	readmetafile.open(chkstr.c_str());
	if (!readmetafile.is_open()){
		cout<< "No meta..Default Heap "<<endl;
	}
	else
	{
		string file_path;
		//  readmetafile.getline(type, 1); //for sorted file also get ordermaker
		getline(readmetafile, file_path);
		getline(readmetafile,type); //for sorted file also get ordermaker

		readmetafile.close();
	}
	// if(!myInternalVar)
	{

		if(type=="0"){
			//		cout<<"Opening heap file"<<endl;
			myInternalVar = new Heap();
		}
		else if (type=="1"){
			//		cout<<"Opening Sorted file"<<endl;
			myInternalVar = new Sort();
		}
		else{
			//			cout<<"Default opening heap file"<<endl;
			myInternalVar = new Heap();
		}
	}
	return (myInternalVar->Open(f_path));

}

void DBFile::MoveFirst () {
	//Forces the current pointer in DBFile to point to the first record in the file
	myInternalVar->MoveFirst();
}

int DBFile::Close () {
	//Simply closes the file. Returns 1 on success and 0 on failure
	myInternalVar->Close();
}

void DBFile::Add (Record &rec) {
	//For the HeapFile implementation, this function simply adds the new record to the end of the file
	//Function should consume rec, so that once rec has been put into the file it cannot be used again
	myInternalVar->Add(rec);
}

void DBFile::AddPage(){
	//Function to add a page into the file
	myInternalVar->AddPage();
}

int DBFile::GetNext (Record &fetchme) {
	//Returns the record next to which the current pointer is pointing, and the current pointer is incremented
	//Returns 0 if the current pointer is pointing to the last record.
	//This function should also handle dirty reads. This happens when CurrentRecord.PageNumber is equal to CurrentFile.GetLength() - 1
	myInternalVar->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	//Returns the next record in the file which is accepted by the selection predicate compared to the literal.
	//The literal is generated when the parse tree for the CNF is processed
	myInternalVar->GetNext( fetchme, cnf, literal);
}
