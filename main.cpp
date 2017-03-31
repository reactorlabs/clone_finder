#include <string>
#include <cmath>
#include <iostream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <sstream>
#include <fstream>
#include <atomic>
#include <thread>
#include <chrono>

/** Shorthand for converting different types to string as long as they support the std::ostream << operator.
*/
#define STR(WHAT) static_cast<std::stringstream&>(std::stringstream() << WHAT).str()

unsigned TOKEN_THRESHOLD = 2;


struct Project {
    unsigned id;
    std::unordered_map<unsigned, unsigned> files;
    int numFiles;

    Project(unsigned id):
        id(id),
        numFiles(0) {
    }

    void addFile(unsigned tokenHash, unsigned tokens) {
        //if (id == 17)
         //   std::cout << tokenHash << " - " << tokens << std::endl;
        ++numFiles;
        if (tokens >= TOKEN_THRESHOLD)
            ++ files[tokenHash];
    }
};

struct FileClone {
    unsigned projectId;
    unsigned numFiles;
    FileClone(unsigned projectId, unsigned numFiles):
        projectId(projectId),
        numFiles(numFiles) {
    }
};


class CloneFinder {
public:
    static void run(std::string const &folder, unsigned threads);

private:

    std::atomic_uint workers_;
    std::atomic_uint done_;

    class Worker;

    friend class Worker;
    CloneFinder(std::string const & folder):
        folder_(folder) {
    }

    void loadData() {
        std::ifstream f(STR(folder_ + "/clone_finder.csv"));
        if (not f.good())
            throw "Cannot open input file";
        std::string line;
        unsigned rows = 0;
        while (std::getline(f, line)) {
            char * x = const_cast<char*>(line.c_str());
            //unsigned fileId = strtol(x, &x , 10);
            unsigned projectId = strtol(x, &x, 10);
            unsigned tokens = strtol(x + 1, &x, 10);
            //unsigned fileHash = strtol(x + 1, &x, 10);
            unsigned tokenHash = strtol(x + 1, &x, 10);
            auto i = projectsMap_.find(projectId);
            Project * p;
            if (i == projectsMap_.end()) {
                p = new Project(projectId);
                projectsMap_[projectId] = p;
                projects_.push_back(p);
            } else {
                p = i->second;
            }
            p->addFile(tokenHash, tokens);
            if (++rows % 10000000 == 0) {
                std::cout << "    " << rows << std::endl;
                //break;
            }
        }
        std::cout << "Total files loaded:    " << rows << std::endl;
        std::cout << "Total projects loaded: " << projects_.size() << std::endl;
    }

    void buildTokenHashProjectIndex() {
        for (Project * p : projects_) {
            for (auto & i : p->files)
                tokenHashProjects_[i.first].push_back(FileClone(p->id, i.second));
        }
        std::cout << "Build tokenHash to projects map" << std::endl;
    }

    std::string folder_;

    std::vector<Project*> projects_;
    std::unordered_map<unsigned, Project*> projectsMap_;
    std::unordered_map<unsigned, std::vector<FileClone>> tokenHashProjects_;

};

class CloneFinder::Worker {
public:



    Worker(unsigned stride, unsigned strideCount, CloneFinder & cf):
        cf_(cf),
        stride_(stride),
        strideCount_(strideCount) {
        w_.open(STR(cf_.folder_ << "/project_clones." << stride << ".csv"));
        if (not w_.good())
            throw "Cannot open output file";
    }

    void run() {
        std::cout << "Worker " << stride_ << std::endl;
        for (size_t i = stride_, e = cf_.projects_.size(); i < e; i += strideCount_) {
            findClonesForProject(i);
            ++cf_.done_;
        }
        w_.close();
        --cf_.workers_;
    }

private:

    void findAllTokenHashClones(unsigned index, Project * p, std::unordered_map<unsigned, std::vector<FileClone>> & files_clones) {
        // at first we look at the current project
        for (auto & i : p->files) {
            if (i.second > 1) // if there are more than one file of the hash in the project
                files_clones[i.first].push_back(FileClone(p->id, i.second - 1));
        }
        // for all our files
        for (auto & i : p->files) {
            unsigned tokenHash = i.first;
            std::vector<FileClone> & clones = files_clones[i.first]; // was i.second
            for (FileClone & fc : cf_.tokenHashProjects_[tokenHash])
                if (fc.projectId > p->id)
                    clones.push_back(fc);
        }
    }

    void findClonesForProject(unsigned index) {
        Project * p = cf_.projects_[index];
        // from token hash to FileClone (i.e. per project info of how many clones of the file hash they have
        std::unordered_map<unsigned, std::vector<FileClone>> files_clones;

        findAllTokenHashClones(index, p, files_clones);

        percentage_clone_projects_counter.clear();
        percentage_host_projects_counter.clear();

        for (auto & i : files_clones) {
            unsigned tokenHash = i.first;
            for (FileClone const & fc : i.second) {
                // how many of this project's files are present in the other project
                size_t x = p->files.size();
                percentage_clone_projects_counter[fc.projectId] += p->files[tokenHash];
                if (x != p->files.size())
                    std::cout << "Another problem";
                // how many of the other's project files are present in this project
                percentage_host_projects_counter[fc.projectId] += fc.numFiles;
            }
        }

        for (auto & i : percentage_host_projects_counter) {
            unsigned k = i.first;
            unsigned v = i.second;
            if (v == 0 or percentage_clone_projects_counter[k] == 0)
                std::cout << "Houston, we have a problem";
            Project * p2 = cf_.projectsMap_[k];
            double percent_cloning = (percentage_clone_projects_counter[k] * 100.0) / p->numFiles;
            double percent_host = v * 100.0 / p2->numFiles;
            // don't store insignificant clones
            if (percent_cloning < 50 && percent_host < 50)
                continue;
            // now output to the database
            // id
            // cloneId
            // cloneClonedFiles
            // cloneTotalFiles
            // cloneCloningPercent
            // hostId
            // hostAffectedFiles
            // hostTotalFiles
            // hostAffectedPercent
            w_ << p->id << ","
               << percentage_clone_projects_counter[k] << ","
               << p->numFiles << ","
               << std::round(percent_cloning * 100) / 100 << ","
               << k << ","
               << v << ","
               << p2->numFiles << ","
               << std::round(percent_host * 100) / 100 << std::endl;
        }
    }

    CloneFinder & cf_;
    unsigned stride_;
    unsigned strideCount_;
    std::ofstream w_;

    std::unordered_map<unsigned, unsigned> percentage_clone_projects_counter;
    std::unordered_map<unsigned, unsigned> percentage_host_projects_counter;

};




void CloneFinder::run(std::string const &folder, unsigned threads) {
    CloneFinder cf(folder);
    cf.loadData();
    cf.buildTokenHashProjectIndex();

    cf.workers_ = threads;
    cf.done_ = 0;
    for (unsigned i = 0; i < threads; ++i) {
        std::thread t([i, threads, & cf] () {
            try {
                Worker w(i, threads, cf);
                w.run();
            } catch (char const * e) {
                std::cerr << "ERROR:" << std::endl;
                std::cerr << e << std::endl;
            }
        });
        t.detach();
    }
    auto start = std::chrono::high_resolution_clock::now();
    while (cf.workers_ != 0) {
        std::this_thread::sleep_for(std::chrono::seconds(10));
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> elapsed = (end-start) / 1000;
        std::cout << "Done " << cf.done_ << " (" << std::round(cf.done_ * 10000.0 / cf.projects_.size()) / 100 << " %) in " << elapsed.count() << " [s]" << std::endl;

    }
}



int main(int argc, char *argv[]) {
    if (argc != 4) {
        std::cerr << "Invalid usage, wrong number of arguments. Use: NUM_THREADS FOLDER TOKEN_THRESHOLD" << std::endl;
        return EXIT_FAILURE;
    }
    int threads = std::atoi(argv[1]);
    std::string folder = argv[2];
    TOKEN_THRESHOLD = std::atoi(argv[3]);
    CloneFinder::run(folder, threads);
    //CloneFinder::run("/data/test", 1);
    return EXIT_SUCCESS;
}
