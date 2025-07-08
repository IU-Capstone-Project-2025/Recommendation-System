
#include "iostream"
#include "fstream"
#include <sstream>
#include "vector"
#include <algorithm> 
using namespace std;

vector<string> Book_Titles_From_CSV;

void read_From_CSV(){
    string filepath = "titles_only.csv";
    ifstream file_for_reading(filepath);
    if (!file_for_reading.is_open()) {
        cerr << "Не удалось открыть файл: "<<endl;
        return;
    }
    string line;
    bool fl =false;
    while (getline(file_for_reading, line)) {
        stringstream ss(line);
        string firstElement;
        
        if (getline(ss, firstElement, '\n')) {
            transform(firstElement.begin(),firstElement.end(),firstElement.begin(),::tolower);
            if (fl) Book_Titles_From_CSV.push_back(firstElement);
        }
        fl = true;
    }

    file_for_reading.close();
    return;
}


void solve(string book_name){
    
}


int main(){
    ios_base::sync_with_stdio(false);
    cin.tie(nullptr);
    read_From_CSV();
    for(int i=0;i<Book_Titles_From_CSV.size();++i){
        cout<<Book_Titles_From_CSV[i]<<endl;
    }
    string book_name;
    cin>>book_name;
    solve(book_name);
    return 0;
}