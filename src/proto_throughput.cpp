/* ========================= eCAL LICENSE =================================
 *
 * Copyright (C) 2016 - 2019 Continental Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * ========================= eCAL LICENSE =================================
*/

#include <ecal/ecal.h>
#include <ecal/msg/protobuf/publisher.h>
#include <ecal/msg/protobuf/subscriber.h>

#include <iostream>

#include "compressed_image.pb.h"

const auto g_snd_loops(2560);

// subscriber callback function
std::atomic<size_t> g_callback_received;
void OnMessage(const foxglove::CompressedImage& msg_)
{
  g_callback_received += msg_.ByteSizeLong();
}

void throughput_test(int snd_loops, bool zero_copy, bool use_payload)
{
  // create publisher
  eCAL::protobuf::CPublisher<foxglove::CompressedImage> pub_proto;
  eCAL::CPublisher                                      pub_raw;
  if (use_payload)
  {
    pub_proto.Create("image");
    pub_proto.ShmEnableZeroCopy(zero_copy);
    pub_proto.ShmSetAcknowledgeTimeout(100);
  }
  else
  {
    pub_raw.Create("image");
    pub_raw.ShmEnableZeroCopy(zero_copy);
    pub_raw.ShmSetAcknowledgeTimeout(100);
  }

  // create subscriber
  eCAL::protobuf::CSubscriber<foxglove::CompressedImage> sub("image");
  // add receive callback function (_2 = msg)
  sub.AddReceiveCallback(std::bind(OnMessage, std::placeholders::_2));

  // let's match them
  eCAL::Process::SleepMS(2000);

  // generate 4 MB protobuf image message
  foxglove::CompressedImage pb_message;
  pb_message.set_format("jpg");
  std::vector<char> data(4*1024*1024);
  pb_message.set_data(data.data(), data.size());
  std::cout << "Message Size : " << pb_message.ByteSizeLong() / 1024 << " kB" << std::endl;

  // buffer to serialize message to
  std::vector<char> msg_buffer(pb_message.ByteSizeLong());

  // initial call to allocate memory file
  if (use_payload)
  {
    pub_proto.Send(pb_message);

  }
  else
  {
    // emulate old behavior
    pb_message.SerializePartialToArray(msg_buffer.data(), int(msg_buffer.size()));
    pub_raw.Send(msg_buffer.data(), msg_buffer.size());
  }

  // reset received bytes counter
  g_callback_received = 0;

  // start time
  auto start = std::chrono::high_resolution_clock::now();

  // do some work
  for (auto i = 0; i < snd_loops; ++i)
  {
    if (use_payload)
    {
      pub_proto.Send(pb_message);
    }
    else
    {
      // emulate old behavior
      pb_message.SerializePartialToArray(msg_buffer.data(), int(msg_buffer.size()));
      pub_raw.Send(msg_buffer.data(), msg_buffer.size());
    }
  }

  // end time
  auto finish = std::chrono::high_resolution_clock::now();
  const std::chrono::duration<double> elapsed = finish - start;
  const size_t sum_snd_bytes = pb_message.ByteSizeLong() * snd_loops;
  const size_t sum_rcv_bytes = g_callback_received;

  std::cout << "Elapsed time : " << elapsed.count() << " s" << std::endl;
  std::cout << "Sent         : " << sum_snd_bytes / (1024 * 1024 * 1024)                               << " GB" << std::endl;
  std::cout << "Lost         : " << sum_snd_bytes - sum_rcv_bytes                                      << " Byte" << std::endl;;
  std::cout << "Latency      : " << elapsed.count() * 1000.0 / double(g_snd_loops)                     << " ms " << std::endl;
  std::cout << "Frequency    : " << int(double(g_snd_loops) / elapsed.count())                         << " Hz " << std::endl;
  std::cout << "Throughput   : " << int((double(sum_snd_bytes) / (1024.0 * 1024.0)) / elapsed.count()) << " MB/s " << std::endl;
}

// main entry
int main(int argc, char** argv)
{
  // initialize eCAL API
  eCAL::Initialize(argc, argv, "pubsub_throughput");

  // publish / subscribe match in the same process
  eCAL::Util::EnableLoopback(true);

  std::cout << "-----------------------------" << std::endl;
  std::cout << "MODE         : BUFFER        " << std::endl;
  std::cout << "LAYER        : SHM           " << std::endl;
  std::cout << "-----------------------------" << std::endl;
  throughput_test(g_snd_loops, false, false);
  std::cout << std::endl;

  std::cout << "-----------------------------" << std::endl;
  std::cout << "MODE         : BUFFER        " << std::endl;
  std::cout << "LAYER        : SHM ZERO-COPY " << std::endl;
  std::cout << "-----------------------------" << std::endl;
  throughput_test(g_snd_loops, true, false);
  std::cout << std::endl;

  std::cout << "-----------------------------" << std::endl;
  std::cout << "MODE         : PAYLOAD       " << std::endl;
  std::cout << "LAYER        : SHM           " << std::endl;
  std::cout << "-----------------------------" << std::endl;
  throughput_test(g_snd_loops, false, true);
  std::cout << std::endl;

  std::cout << "-----------------------------" << std::endl;
  std::cout << "MODE         : PAYLOAD       " << std::endl;
  std::cout << "LAYER        : SHM ZERO-COPY " << std::endl;
  std::cout << "-----------------------------" << std::endl;
  throughput_test(g_snd_loops, true, true);
  std::cout << std::endl;

  // finalize eCAL API
  eCAL::Finalize();

  return(0);
}
