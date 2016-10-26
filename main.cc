#include <iostream>
#include "ParseTree.h"
#include "QueryPlan.h"
#include <fstream>
using namespace std;

extern "C" {
int yyparse(void);   // defined in y.tab.c
}

void createTableCatalog();		//create table
void insertTableCatalog();		//insert into table
void dropTableCatalog(char *);		//drop table
int findTable();				//chk if table exists in catalog
int ConsoleReturnType=0;

//external variables
extern	char *createName; // Create Table name if create command
extern	struct AttList *newAttList; // Attribute list for the table to be created
extern	int commandType; // Type of the command that is select,create insert or delete
extern	char* fileName;
extern	int outType;	//wether to be printed to text, console or 3 for query plan generation
extern  char *outFileName;
extern  char* fileName;


int main () {

	bool flag = true;
	streambuf *sb,*tmp;
	while(flag == true)
	{
		cout<<"----------------------------------------------\n";
		cout<<"Enter command and press cntrl+D\n";
		cout<<"----------------------------------------------\n";
		//parse the command entered
		yyparse();

		//choose whether to create/update/delete based on commandType
		switch (commandType){
		case 1:{

			createTableCatalog();	//create table
			break;
		}
		case 2:{
			insertTableCatalog();		//insert table
			break;
		}
		case 3:
		{
			dropTableCatalog(createName);	//drop a table
			break;
		}
		case 4:
			break;
		case 5:{
			QueryPlan Q;		//select and execute/print query
			Q.start();
			break;
		}
		case 6:{
			flag = false;	//exit
			break;
		}
		default:
			break;

		}
	}
}

//delete Table --- opens catalog and writes into the catalog file.. keeps iterating over the file till it finds table
//name = the one to drop ..then skip those while rewriting into the file
void dropTableCatalog(char* tablename){
	string line,tmpline;
	int i=0;
	bool flag=false;
	bool found=false;
	bool ffound=false;

	ifstream infile("catalog");
	ofstream outtmpfile("tmp");

	while(!infile.eof()){
		getline(infile,line);	//check line by line for existance of table name
		if (strcmp((char*)line.c_str(),"BEGIN")==0){
			tmpline = line;
			getline(infile,line);
			flag = true;
		}
		if (strcmp(createName,(char*)line.c_str())==0){	//if table found ..hav to drop it
			found=true;									//dont write in file
			ffound=true;
			flag = false;
			while(strcmp((char*)line.c_str(),"END")!=0){
				getline(infile,line);
			}
		}
		else{
			if (flag == true){				//dont write in file
				outtmpfile.write((char*)tmpline.c_str(),tmpline.	length());
				outtmpfile.put('\n');
				flag = false;
			}
		}
		if (found==true && (strcmp((char*)line.c_str(),"END")==0)){			//if not found in entire file
			found=false;			//error
			continue;
		}
		outtmpfile.write((char*)line.c_str(),line.length());
		outtmpfile.put('\n');
	}
	infile.close();
	outtmpfile.close();

	if(!ffound){
		cout<< "\nTable not found"<<endl;
		remove("tmp");
		ConsoleReturnType= -1;
	}

	remove("catalog");
	rename("tmp","catalog");
	if (ConsoleReturnType ==-1)
	{
		cout<<"\nDrop table failed"<<endl;
		return;
	}
	else
	{
		strcat(createName,".bin");
		remove(createName);
		strcat (createName,".header");
		remove(createName);

		cout << "\nDrop table completed"<<endl;
	}
}

//gets catalog and writes table details into the catalog
void insertTableCatalog()
{
	DBFile *myDBFile = new DBFile();
	Schema *mysch = new Schema ("catalog", createName);
	if (findTable()==-1){
	//	createTableCatalog();
		strcat(createName,".bin");
		myDBFile->Open(createName);
		myDBFile->Load(*mysch,fileName);
		myDBFile->Close();
		cout << "INSERT table successful"<<endl;
	}
	else
		cout<< "INSERT table failed"<<endl;
}


void createTableCatalog(){
	string line;
	char *tmp = createName;
	AttList *tmpAttList = newAttList;

	if(findTable()== -1)
	{
		ConsoleReturnType = -1;

	}
	ifstream infile ("catalog");
	ofstream outfile("catalog",ios_base::app);

	//write in specified format
	outfile.write("BEGIN\n",6);	//begin tablename aatributes with attribute type end
	while(*(tmp)!='\0'){
		outfile.put(*(tmp));
		tmp++;
	}
	outfile.put('\n');
	tmp = createName;
	while(*(tmp)!='\0'){
		outfile.put(*(tmp));
		tmp++;
	}
	outfile.write(".tbl\n",5);
	while(tmpAttList != NULL){	//put all attr in file
		tmp = tmpAttList->AttInfo->name;
		while(*(tmp)!='\0'){			//attribute name
			outfile.put(*(tmp));
			tmp++;
		}
		if (tmpAttList->AttInfo->code == INT)	//attribute type
			outfile.write(" Int\n",4);
		else if (tmpAttList->AttInfo->code == DOUBLE)
			outfile.write(" Double\n",7);
		else if (tmpAttList->AttInfo->code == STRING)
			outfile.write(" String\n",7);
		outfile.put('\n');
		tmpAttList = tmpAttList->next;
	}
	outfile.write("END\n",4);
	infile.close();
	outfile.close();
	DBFile *DBobj = new DBFile();
	if (ConsoleReturnType ==-1)
	{
		cout << "Create table failed\n";
		return;
	}
	Schema *mysch = new Schema ("catalog", createName);
	strcat(createName,".bin");

	DBobj->Create(createName,heap,NULL);
	DBobj->Close();

	cout << "\nCreated table successfully"<<endl;
}

int findTable()
{
	string line;
	char *tmp = createName;
	ifstream infile ("catalog");
	ofstream outfile("catalog",ios_base::app);

	while(!infile.eof()){
		getline(infile,line);
		if (line.compare((string)createName)==0){
			cout << "Table already exists\n";
			infile.close();
			outfile.close();
			return -1;
		}
	}
	infile.close();
	outfile.close();

	return 0;
}
