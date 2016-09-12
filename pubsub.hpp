//
// Created by meox on 27/08/16.
//

#ifndef PUBSUB_PUBSUB_HPP_H
#define PUBSUB_PUBSUB_HPP_H

#include <memory>
#include <vector>
#include <thread>
#include <map>
#include <mutex>
#include <atomic>

#include <boost/lockfree/policies.hpp>
#include <boost/lockfree/queue.hpp>
#include <set>


namespace ps
{
	template <typename T>
	class Subscriber;

	template <typename T>
	class Topic;

	template <typename T>
	using topic_ptr_t = std::shared_ptr<Topic<T>>;

	template <typename T>
	using subscriber_ptr_t = std::shared_ptr<Subscriber<T>>;


	template <typename T>
	class Publisher
	{
	public:
		Publisher(const topic_ptr_t<T>& topic) : _topic{topic}
		{
			_topic->attach(this);
		}

		void produce(T&& msg)
		{
			_topic->send(std::forward<T>(msg));
		}

		virtual void signal(int type_signal) {}

		size_t get_id() const {	return id; }
		virtual ~Publisher()
		{
			_topic->detach(this);
		}

	private:
		static size_t get_counter()
		{
			static std::size_t counter{};
			counter++;
			return counter;
		}

	private:
		const size_t id{get_counter()};
		topic_ptr_t<T> _topic;
	};


	template <typename T>
	using publisher_ptr_t = std::shared_ptr<Publisher<T>>;


	template <typename T>
	class Topic
	{
	public:
		Topic() = default;

		Topic(std::string name) : _name{std::move(name)}
		{}

		void send(T&& data)
		{
			for(auto& sub : subs)
				sub.second->deliver(this, data);
		}

		void attach(Publisher<T>* s)
		{
			std::lock_guard<std::mutex> l(m);
			pubs[s->get_id()] = s;
		}

		void detach(Publisher<T>* s)
		{
			std::lock_guard<std::mutex> l(m);
			pubs.erase(s->get_id());
		}

		void subscribe(Subscriber<T>* s)
		{
			std::lock_guard<std::mutex> l(m);
			subs[s->get_id()] = s;
			signals[s->get_id()].clear();
		}

		void unsubscribe(Subscriber<T>* s)
		{
			std::lock_guard<std::mutex> l(m);
			subs.erase(s->get_id());
			signals.erase(s->get_id());
		}

		void signal(int type_signal, size_t subscriber_id)
		{
			std::lock_guard<std::mutex> l(m);
			signals[subscriber_id].insert(type_signal);

			bool r = std::all_of(signals.begin(), signals.end(), [type_signal](const auto& e){
				return e.second.find(type_signal) != e.second.end();
			});

			if (r)
			{
				for (auto& e : signals)
					e.second.erase(type_signal);

				for (auto& p : pubs)
					p.second->signal(type_signal);
			}
		}

		size_t get_id() const {	return id; }

	private:
		static size_t get_counter()
		{
			static std::size_t counter{};
			counter++;
			return counter;
		}

		std::mutex m;
		size_t id{get_counter()};
		std::string _name{};
		std::map<size_t, Subscriber<T>*> subs;
		std::map<size_t, Publisher<T>*> pubs;
		std::map<size_t, std::set<int>> signals{};
	};


	template <typename T>
	struct msg_container_t
	{
		const Topic<T>* topic_ptr;
		T data;
	};


	template <typename T>
	class Subscriber
	{
	public:
		using f_callback_t = std::function<void(const Topic<T>* topic, T data)>;
		using queue_t = boost::lockfree::queue<msg_container_t<T>, boost::lockfree::capacity<32000ul>>;

		using topic_raw_ptr = const Topic<T>*;
		using data_t = const T&;

		Subscriber() = default;
		Subscriber(f_callback_t f) : callaback{std::move(f)}
		{}

		void subscribe(const std::vector<topic_ptr_t<T>>& topics)
		{
			for (const auto& topic : topics)
				subscribe(topic);
		}

		void subscribe(const std::initializer_list<topic_ptr_t<T>>& topics)
		{
			for (const auto& topic : topics)
				subscribe(topic);
		}

		void subscribe(const std::vector<topic_ptr_t<T>>& topics, f_callback_t f)
		{
			callaback = std::move(f);
			for (const auto& topic : topics)
				subscribe(topic);
		}

		void subscribe(const topic_ptr_t<T>& topic)
		{
			topic->subscribe(this);
			topics[topic->get_id()] = topic;
		}

		void unsubscribe(const topic_ptr_t<T>& topic)
		{
			topic->unsubscribe(this);
			topics.erase(topic->get_id());
		}

		virtual void deliver(topic_raw_ptr topic, data_t e)
		{
			msg_container_t<T> msg{topic, e};

			while (true)
			{
				bool b = data.push(msg);
				if (b)
					break;
				else
					std::this_thread::sleep_for(std::chrono::milliseconds(10));
			}
		}

		void run()
		{
			stopped = false;
			th = std::thread([this](){
				event_loop();
			});
		}

		void wait()
		{
			if (!stopped)
				th.join();
		}

		void stop()
		{
			if (!stopped)
			{
				stopped = true;
				th.join();
			}
		}

		size_t get_id() const {	return id; }
		virtual ~Subscriber() {	stop(); }

	protected:
		static size_t get_counter()
		{
			static std::size_t counter{};
			counter++;
			return counter;
		}

		virtual void event_loop()
		{
			msg_container_t<T> msg;
			bool at_least_one{false};
			bool mute_nodata{false};

			while (true)
			{
				bool data_arrived{false};
				const auto is_data = data.pop(msg);
				if (is_data)
				{
					mute_nodata = false;
					at_least_one = true;
					data_arrived = true;
					execute(msg.topic_ptr, msg.data);
				}

				if (!data_arrived)
				{
					if (stopped)
						break;

					if (at_least_one && !mute_nodata)
					{
						emit_signal(0);
						mute_nodata = true;
					}

					std::this_thread::sleep_for(std::chrono::milliseconds(50));
				}
			}
		}

		void emit_signal(int type_signal)
		{
			for (auto& t : topics)
				t.second->signal(type_signal, get_id());
		}

		virtual void execute(topic_raw_ptr topic, data_t data)
		{
			if(callaback)
				callaback(topic, data);
		}

	private:
		f_callback_t callaback;
		queue_t data;
		std::atomic<bool> stopped{true};

		std::map<size_t, topic_ptr_t<T>> topics;
		std::thread th;
		const size_t id{get_counter()};
	};


	template <typename T>
	topic_ptr_t<T> create_topic(const std::string& name)
	{
		return std::make_shared<Topic<T>>(name);
	}

	template <typename T>
	publisher_ptr_t<T> create_publisher(topic_ptr_t<T> topic)
	{
		return std::make_shared<Publisher<T>>(topic);
	}

	template <typename T, typename Q>
	publisher_ptr_t<T> create_publisher(topic_ptr_t<T> topic)
	{
		return std::make_shared<Q>(topic);
	}

	template <typename T>
	subscriber_ptr_t<T> create_subscriber(const topic_ptr_t<T>& topic, typename Subscriber<T>::f_callback_t&& f)
	{
		auto s = std::make_shared<Subscriber<T>>(f);
		s->subscribe(topic);
		return s;
	}

	template <typename T, typename F>
	subscriber_ptr_t<T> create_subscriber(const std::vector<topic_ptr_t<T>>& topics, F&& f)
	{
		auto s = std::make_shared<Subscriber<T>>(f);
		s->subscribe(topics);
		return s;
	}

	template <typename T, typename F>
	subscriber_ptr_t<T> create_subscriber(const std::initializer_list<topic_ptr_t<T>>& topics, F&& f)
	{
		std::vector<topic_ptr_t<T>> v_t{topics};
		return create_subscriber(v_t, std::forward<F>(f));
	}
}

#endif //PUBSUB_PUBSUB_HPP_H
