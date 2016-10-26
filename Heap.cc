#include "Heap.h"
#include <assert.h>
#include <iostream>
#include <stdlib.h>



extern "C"{
	int yyparse(void);
}

extern struct AndList *final;

Heap::Heap () {
	//Initialize current record pointer to point to the first record in the file.
	MoveFirst();
	DirtyPageAdded = true;
	Adder = new Page();
}

int Heap::Create (char *f_path,void *startup) {
	//Use the File Class from File.h to store the actual database data.
	//Any meta-data needed can be stored in an associated file as <filename>.header
	//ftype is either heap, sorted or tree - enum defined in DBFile.h
	//f_path is the location
	//Returns 1 on success and 0 on failure.
	int Returnvalue=0;
	CurrentFile.Open(0, f_path);
	Returnvalue = 1; //Directly setting this value as 1 because Open exits if file cannot be opened
	/*switch(f_type){
	case 0: CurrentFile.Open(0, f_path);
			Returnvalue = 1; //Directly setting this value as 1 because Open exits if file cannot be opened
			break;
	case 1: break;
	case 2: break;
	}*/
	return(Returnvalue);

	//*startup is a dummy parameter not used in Assignment 1
}

void Heap::Load (Schema &f_schema, char *loadpath) {
	//Bulk loads the DBFile instance from a text file, appending new data to it using the SuckNextRecord function
	//loadpath is the name of the data file to bulk load
	Record temp;
	FILE *tablefile = fopen(loadpath, "r");
	while(temp.SuckNextRecord(&f_schema, tablefile) == 1){
		Add(temp);
	}
	//Add the last remaining things in the buffer
	if(!AllRecords.empty()){
		assert(Adder != NULL);
		CurrentFile.AddPage(Adder, AllRecords.at(AllRecords.size() - 1).PageNumber);
		cout<<"Last Page added"<<endl;
		Adder = new Page();
	}
	else{
		assert(Adder != NULL);
		CurrentFile.AddPage(Adder, 0);
		Adder = new Page();
	}
}

int Heap::Open (char *f_path) {

	//Function assumes that the DBFile has been created and closed before.
	//If any other parameters are needed, they should be written into the auxillary .header file before which should also be opened at startup
	//Returns 1 on success and 0 on failure
	int Returnvalue=0;
	CurrentFile.Open(1, f_path);
	Returnvalue = 1; //Directly setting this 1 because Open exits if file cannot be opened
	return(Returnvalue);
}

void Heap::MoveFirst () {
	//Forces the current pointer in DBFile to point to the first record in the file
	CurrentRecord.PageNumber = 0;
	CurrentRecord.PageOffset = 0;
}

int Heap::Close () {
	//Simply closes the file. Returns 1 on success and 0 on failure
	int Returnvalue = 0;
	AddPage();
	Returnvalue = CurrentFile.Close();
	return(Returnvalue);
}

void Heap::Add (Record &rec) {
	//For the HeapFile implementation, this function simply adds the new record to the end of the file
	//Function should consume rec, so that once rec has been put into the file it cannot be used again
	int Returnvalue = 0;
	DirtyPageAdded = false;
	if(Adder == NULL){
		Adder = new Page();
	}
	//cout<<"In Add"<<endl;
	Returnvalue = Adder->Append(&rec); //Returns 1  on success and 0 if page is full.
	if(Returnvalue == 0){
		//cout<<"Could not append"<<endl;
		AddPage();
		Returnvalue = Adder->Append(&rec); //Add the record again.
		assert(Returnvalue == 1);
	}
	else{//Record has been appended successfully
		if(AllRecords.empty()){//If the vector is empty, then create a new record and add it to the vector
			RecPointer newRec;
			newRec.PageNumber=0;
			newRec.PageOffset=0;
			AllRecords.push_back(newRec);
			//cout<<"Pushed new rec to AllRecords "<<AllRecords.size()<<endl;
		}
		else{
			AllRecords.at(AllRecords.size()-1).PageOffset++; //Increment the page offset of the last record
		}
	}
}

void Heap::AddPage(){
	//Function to add a page into the file
	RecPointer newRec;
	if(!AllRecords.empty()){
		newRec.PageNumber = AllRecords.at(AllRecords.size() - 1).PageNumber + 1;
	}
	else{
		newRec.PageNumber = 0;
	}
	newRec.PageOffset = 0;
	assert(Adder != NULL);
	CurrentFile.AddPage(Adder, newRec.PageNumber); //Page is full, so add it to file
	Adder = new Page();
	//cout<<"Added Page"<<endl;
	//Page details are pushed into AllRecords only after the page is full and a new page is created.
	AllRecords.push_back(newRec);
	//CurrentPage.EmptyItOut(); //Empty the page
	//cout<<CurrentRecord.PageNumber<<" "<<CurrentRecord.PageOffset<<endl;
}

int Heap::GetNext (Record &fetchme) {
	//Returns the record next to which the current pointer is pointing, and the current pointer is incremented
	//Returns 0 if the current pointer is pointing to the last record.
	//This function should also handle dirty reads. This happens when CurrentRecord.PageNumber is equal to CurrentFile.GetLength() - 1
	int Returnvalue=0;
	int j=0;
	off_t i=0;
	//cout << CurrentFile.GetLength()<<" "<<CurrentRecord.PageNumber<<endl;
/*	if(CurrentFile.GetLength() - 1 == CurrentRecord.PageNumber){
		//cout << CurrentFile.GetLength()<<" "<<CurrentRecord.PageNumber<<endl;
		return(Returnvalue);
	}
	if((CurrentFile.GetLength() - 2 == CurrentRecord.PageNumber)&&(!DirtyPageAdded)){
		//Trying to access the Currentpage which hasnt been pushed into the file yet
		//So first push it into the file
		AddPage();
		DirtyPageAdded = true;
	}*/
	if(CurrentRecord.PageOffset == 0){ //Fetch new page if the page offset is 0 which means a new page needs to be fetched

		//CurrentFile.GetPage(&CurrentPage, CurrentRecord.PageNumber); //Puts the fetched page in CurrentPage
		Adder = new Page();
		Adder->EmptyItOut();
		CurrentFile.GetPage(Adder, CurrentRecord.PageNumber);
	}
	//j = CurrentPage.GetFirst(&fetchme);
	//assert(Adder != NULL);
	j = Adder->GetFirst(&fetchme);
	//If no record has been returned, increment page number, set page offset to 0, and read new page and first record
	if(j == 0){
		CurrentRecord.PageNumber++;
		CurrentRecord.PageOffset=0;
		if(CurrentFile.GetLength() - 1== CurrentRecord.PageNumber)
			return(Returnvalue);
		if(((CurrentFile.GetLength() - 2) == CurrentRecord.PageNumber)&&(!DirtyPageAdded)){
			//Trying to access the CurrentPage which hasnt been pushed into the file yet
			//So first push it into the file
			AddPage();
			DirtyPageAdded = true;
		}
		//CurrentPage.EmptyItOut();
		Adder = new Page();
		Adder->EmptyItOut();
		//CurrentFile.GetPage(&CurrentPage, CurrentRecord.PageNumber);
		CurrentFile.GetPage(Adder, CurrentRecord.PageNumber);
		//*Adder = CurrentPage;
		//j = CurrentPage.GetFirst(&fetchme);
		assert(Adder != NULL);
		j = Adder->GetFirst(&fetchme);
	}
	if(j == 1){
		//Put the next record in the location where it has been asked
		CurrentRecord.PageOffset++;
		Returnvalue = 1;
	}
	return(Returnvalue);
}

int Heap::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	//Returns the next record in the file which is accepted by the selection predicate compared to the literal.
	//The literal is generated when the parse tree for the CNF is processed
	ComparisonEngine comp; //Used to compare the literal with the record
	//cout<<lineitem<<" "<<literal<<endl;
	int j=0;
	while(true){
		j = GetNext(fetchme);

		if(j == 1){
			if(comp.Compare(&fetchme, &literal, &cnf)){
				return(1);
			}
		}
		else
			return 0;
	}
}
