
/*#include <iostream>
#include "ParseTree.h"

using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

int main () {

	yyparse();
}*/


#include <iostream>
#include "ParseTree.h"
#include "QueryPlan.h"
#include <fstream>
using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}
//extern struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
//extern 	struct TableList *tables; // the list of tables and aliases in the query
//extern 	struct AndList *boolean; // the predicate in the WHERE clause
//extern 	struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
//extern 	struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
//extern 	int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query
//extern	int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query

extern	char *createName; // Create Table name
extern	struct AttList *newAttList; // Attribute list for the table to be created
extern	int commandType; // Type of the command
extern	char* fileName;
extern	int outType;
extern  char *outFileName;
extern  char* fileName;

int update_catalog();
int delete_from_catalog(char *);
int main () {
	int ret=0;
	bool flag = true;
	streambuf *sb,*tmp;
	while(flag == true){
	cout<<"Enter command and then press cntrl+D\n";
	yyparse();
	switch (commandType){
		case 1:{
			DBFile *myDBFile = new DBFile();
			ret = update_catalog();
			if (ret ==-1){
				cout << "CREATE TABLE: FAILED\n";
				break;
			}
			Schema *mysch = new Schema ("catalog", createName);
			strcat(createName,".bin");
			myDBFile->Create(createName,heap,NULL);
			cout << "CREATE TABLE: SUCCESSFUL"<<endl;
			myDBFile->Close();
			break;
			}
		case 2:{
			DBFile *myDBFile = new DBFile();
			Schema *mysch = new Schema ("catalog", createName);
			if (update_catalog()==-1){
				strcat(createName,".bin");
				myDBFile->Open(createName);
				myDBFile->Load(*mysch,fileName);
				myDBFile->Close();
				cout << "INSERT tablefile INTO table: SUCCESSFUL"<<endl;
			}
			else
				cout<< "INSERT tablefle INTO table: FAILED"<<endl;
			break;
			}
		case 3:{
			ret = delete_from_catalog(createName);
			if (ret ==-1)
			{
				cout<<"DROP TABLE tablename: FAILED"<<endl;
				break;
			}
			strcat(createName,".bin");
			remove(createName);
			strcat (createName,".header");
			remove(createName);
			cout << "DROP TABLE tablename: SUCCESSFUL"<<endl;
			break;
			}
		case 4:
			break;
		case 5:{
			QueryPlan Q;
			Q.start();
			// Display output of the query
			break;
			}
		case 6:{
			flag = false;
			break;
			}
		default:
			break;

	}
	}
}
int delete_from_catalog(char* tablename){
	//if (update_catalog()!=-1){
	//	cout<< "Table not present"<<endl;
	//	return -1;
	//}
	string line;
	string tmpline;
	int i=0;
	bool flag=false;
	bool found=false;
	bool ffound=false;
	ifstream infile("catalog");
	ofstream outtmpfile("tmp");
	while(!infile.eof()){
		getline(infile,line);
		if (strcmp((char*)line.c_str(),"BEGIN")==0){
			tmpline = line;
			getline(infile,line);
			flag = true;
		}
		if (strcmp(createName,(char*)line.c_str())==0){
			found=true;
			ffound=true;
			flag = false;
			while(strcmp((char*)line.c_str(),"END")!=0){
				getline(infile,line);
			}
		}
		else{
			if (flag == true){
				outtmpfile.write((char*)tmpline.c_str(),tmpline.length());
				outtmpfile.put('\n');
				flag = false;
			}
		}
		if (found==true && (strcmp((char*)line.c_str(),"END")==0)){
		found=false;
		continue;
		}
		outtmpfile.write((char*)line.c_str(),line.length());
		outtmpfile.put('\n');
	}
	infile.close();
	outtmpfile.close();
	if(!ffound){
                cout<< "Table not found"<<endl;
		remove("tmp");
                return -1;
        }
	remove("catalog");
	rename("tmp","catalog");
}
int update_catalog(){
	string line;
	char *tmp = createName;
	AttList *tmpAttList = newAttList;
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
	outfile.write("BEGIN\n",6);
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
	while(tmpAttList != NULL){
		tmp = tmpAttList->AttInfo->name;
		while(*(tmp)!='\0'){
			outfile.put(*(tmp));
			tmp++;
		}
		if (tmpAttList->AttInfo->code == INT)
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
	return 1;
}



