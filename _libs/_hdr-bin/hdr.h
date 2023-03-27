#pragma once 

#include <iostream>
#include <math.h>
#include <cmath>
#include <vector>
#include <fstream>
#include <sstream>

struct Header {
	std::vector<float> info;
	std::vector<long long> start_end;
	std::vector<std::string> featurenames;
	std::vector<double> lsbs;
	std::vector<std::string> units;

	Header() {}

	void ReadFile(std::string fname) {
		std::ifstream in(fname);
		if (!in.is_open()) {
			std::cerr << "[ERROR]: .hdr file didn't found!!!" << std::endl;
			return;
		}
		ReadFile(in);
	}

	void ReadFile(std::istream &stream) {
		std::string type = "int32";    // тип данных
		double freq = 0;            // частота
		double lsb = 0;             // вес младшего разряда
		time_t startPoint = 0;      // первая точка
		time_t endPoint = 0;        // ? последняя точка сигнала
		std::string realTime;            // реальное время

		int fieldnum;
		std::string line;
		std::getline(stream, line);
		auto tmpstream = std::istringstream(line);
		tmpstream >> fieldnum >> freq >> lsb; // >> type;

		info.push_back(fieldnum);
		info.push_back(freq);
		info.push_back(lsb);

		featurenames.resize(fieldnum + 1);
		lsbs.resize(fieldnum);
		units.resize(fieldnum);

		stream >> startPoint >> endPoint >> featurenames[0];
		start_end.push_back(startPoint);
		start_end.push_back(endPoint);

		for (int i = 0; i < fieldnum; i++) stream >> featurenames[i + 1];
		for (int i = 0; i < fieldnum; i++) stream >> lsbs[i];
		for (int i = 0; i < fieldnum; i++) stream >> units[i];
	}

	Header(std::istream &stream) {
		ReadFile(stream);
	}

	Header(std::string fname) {
		ReadFile(fname);
	}

	Header(const Header &H) {
		this->info = H.info;
		this->start_end = H.start_end;
		this->featurenames = H.featurenames;
		this->lsbs = H.lsbs;
		this->units = H.units;
	}

	Header(const Header &H, int num_channels) {
		this->info = H.info;
		this->start_end = H.start_end;
		this->featurenames = H.featurenames;
		this->lsbs = H.lsbs;
		this->units = H.units;
		std::vector<std::string> new_names;
		if (num_channels == 12) {
			new_names = {"I", "II", "III", "AVR", "AVL", "AVF", "V1", "V2", "V3", "V4", "V5", "V6"};
		}

		this->info[0] = num_channels;

		for (int i = 0; i < num_channels; i++) {
        	if (i + 1 < H.featurenames.size()) {
        	    this->featurenames[i + 1] = new_names[i];
        	} else {
        	    this->featurenames.push_back(new_names[i]);
        	    this->lsbs.push_back(this->lsbs[0]);
        	    this->units.push_back(this->units[0]);
        	}
    	}		
	}

	void Write(std::ostream &output) {
		int CNT = featurenames.size();

		for (int i = 0; i < 3; i++) {
			output << info[i];
			if (i < 2) {
				output << " ";
			}
		}
		output << std::endl;
		output << start_end[0] << "\t" << start_end[1] << " " << featurenames[0] << std::endl;
		for (int i = 1; i < CNT; i++) {
			output << featurenames[i];
			if (i < CNT - 1) {
				output << "\t";
			}
		}
		output << std::endl;
		for (int i = 0; i < CNT - 1; i++) {
			output << lsbs[i];
			if (i < CNT - 2) {
				output << "\t";
			}
		}
		output << std::endl;
		for (int i = 0; i < CNT - 1; i++) {
			output << units[i];
			if (i < CNT - 2) {
				output << "\t";
			}
		}
	}
};

Header ReadWriteHeader(std::istream &in, std::ostream &out) {
	Header Head(in); 
    Head.Write(out);
    char useless[2];
	in.read((char *)useless, 1);
    out << "\n";

	return Head;
}
