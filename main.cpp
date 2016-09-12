#include <iostream>
#include <fstream>
#include "pubsub.hpp"

using namespace ps;


struct global_sub : Subscriber<std::string*>
{
	void execute(topic_raw_ptr topic, data_t data) override
	{
		counter++;

		if (counter == 10)
			std::cout << *data << "\n";
	}

	size_t counter{0};
};


struct custom_publisher : Publisher<std::string*>
{
	using Publisher<std::string*>::Publisher;
};

std::vector<std::string*> v_str;
std::string* push_data(const std::string& e)
{
	v_str.push_back(new std::string(e));
	return v_str[v_str.size() - 1];
}


int main()
{
	auto meteo = create_topic<std::string*>("meteo");
	auto meteo_station = create_publisher(meteo);

	auto temp = create_topic<std::string*>("temp");
	auto season = create_publisher(temp);

	auto snd = create_publisher<std::string*, custom_publisher>(temp);

	size_t web_news_counter{};
	auto web_news = create_subscriber(meteo, [&web_news_counter](const Topic<std::string*>* topic, const std::string* data){
		web_news_counter++;
	});

	size_t ansa_counter{};
	auto ansa = create_subscriber(meteo, [&ansa_counter](const Topic<std::string*>* topic, const std::string* data){
		ansa_counter++;
	});


	global_sub global;
	global.subscribe({meteo, temp});

	web_news->run();
	ansa->run();
	global.run();

	std::thread th_meteo([&]{
		std::vector<std::string> cities{"Rome", "Florence", "Venice"};

		for (uint32_t i = 0; i < 1000000; i++)
		{
			const std::string tcelsius = std::to_string(24 + (i % 10));
			meteo_station->produce(push_data(tcelsius));
			season->produce(push_data(cities[i%3] + ", " + tcelsius));
		}
	});

	th_meteo.join();

	web_news->stop();
	ansa->stop();
	global.stop();

	std::cout << "web_news_counter: " << web_news_counter << std::endl;
	std::cout << "ansa_counter: " << ansa_counter << std::endl;
	std::cout << "g_counter: " << global.counter << std::endl;

	return 0;
}
