
#include "iostream"
#include "fstream"
#include <sstream>
#include "vector"
#include <queue>
#include <algorithm> 
#include <unordered_map>
using namespace std;

vector<string> Book_Titles_From_CSV;
vector<string> ans;
unordered_map<string,string> Map_With_Most_Similar_Books;

struct Comparator {
    bool operator ()(const pair<string,int>& a, const pair<string,int>&b){
        return a.second > b.second;
    }
};

priority_queue<pair<string, int>, vector<pair<string,int> >, Comparator> Priority_Queue_With_Most_Similar_Books;

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
            string temp_value(firstElement);
            transform(firstElement.begin(),firstElement.end(),firstElement.begin(),::tolower);
            string v;
            for(char c: firstElement) if (c != ' ') v += c;
            Map_With_Most_Similar_Books[v]=temp_value;
            if (fl) Book_Titles_From_CSV.push_back(v);
        }
        fl = true;
    }

    file_for_reading.close();
    return;
}


void solve(string book_name){
    transform(book_name.begin(),book_name.end(), book_name.begin(),::tolower);

    string v;
    for(char c: book_name) if (c != ' ') v += c;
    book_name = v;

    for(auto book_from_csv: Book_Titles_From_CSV){
        
        int matrix[book_from_csv.length()+1][book_name.length()+1];
        
        for(int i=0;i<book_from_csv.length()+1;++i){
            matrix[i][0]=i;
        }
        for(int i=0;i<book_name.length()+1;++i){
            matrix[0][i]=i;
        }
        
        for(int i=1; i<=book_from_csv.length(); ++i){
            for(int j=1; j<=book_name.length();++j){
                int cost; 
                if (book_from_csv[i-1]==book_name[j-1] ) cost =0;
                else cost = 1;
                matrix [i][j] = min(
                    matrix[i-1][j]+1,
                    min(
                        matrix[i][j-1]+1,
                        matrix[i-1][j-1]+cost
                    )
                );
                if (i>1 && j>1 && book_from_csv[i-1]==book_name[j-2]&& book_from_csv[i-2]==book_name[j-1]){
                    matrix[i][j] = min(matrix[i][j], matrix[i-2][j-2]+cost);
                }
            }
        }
        
        Priority_Queue_With_Most_Similar_Books.push(make_pair(book_from_csv,matrix[book_from_csv.length()][book_name.length()]));
    }
    pair<string,int> first_elem_in_pq = Priority_Queue_With_Most_Similar_Books.top();
  
    int cnt =9;
    while(cnt-->0){
        ans.push_back(Priority_Queue_With_Most_Similar_Books.top().first);
        Priority_Queue_With_Most_Similar_Books.pop();
    }
}


int main(){
    ios_base::sync_with_stdio(false);
    cin.tie(nullptr);
    read_From_CSV();

    string book_name;
    getline(cin,book_name);

    if (book_name.length()>100) {
        cerr<<"Book name is too long"<<endl; 
        return 0;
    }
    solve(book_name);
    for(int i=0;i<ans.size();++i){
        cout<<Map_With_Most_Similar_Books[ans[i]]<<endl;
    }
    return 0;
}