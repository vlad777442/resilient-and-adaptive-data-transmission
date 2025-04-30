import simpy
import random
from formulaModule import TransmissionTimeCalculator
import math
import datetime

SIM_DURATION = 1000
CHUNK_BATCH_SIZE = 10 # Number of chunks after which the sender sends a control message

class Link:
    """This class represents the data transfer through a network link."""

    def __init__(self, env, delay):
        self.env = env
        self.delay = delay
        self.packets = simpy.Store(env)
        self.loss = simpy.Store(env)

    def transfer(self, value):
        yield self.env.timeout(self.delay)
        if len(self.loss.items) and value["type"] != "last_fragment" and value["type"] != "control":
            loss = yield self.loss.get()
            print(f'{loss}, {value} got dropped')
        else:
            yield self.packets.put(value)

    def put(self, value):
        self.env.process(self.transfer(value))

    def get(self):
        return self.packets.get()

class Sender:

    def __init__(self, env, link, rate, tier_frags_num, tier_m, n, calculator):
        self.env = env
        self.link = link
        self.rate = rate
        self.tier_frags_num = tier_frags_num
        self.tier_m = tier_m
        self.n = n
        self.start_time = None
        self.number_of_chunks = []
        self.fragments_sent = 0
        self.control_messages = simpy.Store(env) 
        self.calculator = calculator
        self.lambdas = []
        self.lambda_file = open("lambdas.txt", "w")
        self.total_fragments_sent = 0
        self.total_fragments = sum(self.tier_frags_num)

    def send(self):
        """A process which generates and sends fragments by chunk."""
        if self.start_time is None:
            self.start_time = self.env.now

        for t in range(len(self.tier_frags_num)):
            frags_num = int(self.tier_frags_num[t])
            last_chunk_id = frags_num // (self.n - self.tier_m[t])
            total_chunks = last_chunk_id + 1
            batch_counter = 0 

            for chunk_id in range(total_chunks):
                data_frags, parity_frags = self.generate_chunk_fragments(t, chunk_id, frags_num)
                # print(f"Tier {t}, chunk {chunk_id}, data fragments: {len(data_frags)}, parity fragments: {len(parity_frags)}")
                self.fragments_sent += len(data_frags) + len(parity_frags)
                for frag in data_frags + parity_frags:
                    yield self.env.timeout(1.0 / self.rate)
                    frag["time"] = self.env.now
                    self.link.put(frag)
                    self.total_fragments_sent += 1

                batch_counter += 1
                
                if batch_counter == CHUNK_BATCH_SIZE or chunk_id == last_chunk_id:
                    control_msg = {"tier": t, "chunk": chunk_id, "type": "control", "fragments_sent": self.fragments_sent}
                    self.link.put(control_msg)
                    self.fragments_sent = 0
                    batch_counter = 0

            self.number_of_chunks.append(total_chunks)

        last_frag = {"tier": -1, "chunk": 0, "fragment": 0, "type": "last_fragment"}
        self.link.put(last_frag)
    
    def generate_chunk_fragments(self, tier, chunk_id, frags_num):
        """Generate data and parity fragments for a given chunk."""
        data_frags = []
        parity_frags = []

        start_idx = chunk_id * (self.n - self.tier_m[tier])
        end_idx = min(start_idx + (self.n - self.tier_m[tier]), frags_num)
        # print("start_idx", start_idx, end_idx)
        for i in range(start_idx, end_idx):
            fragment = {"tier": tier, "chunk": chunk_id, "fragment": i - start_idx, "type": "data", "k": end_idx - start_idx}
            data_frags.append(fragment)

        for p in range(self.tier_m[tier]):
            fragment = {"tier": tier, "chunk": chunk_id, "fragment": len(data_frags) + p, "type": "parity", "m": self.tier_m[tier]}
            parity_frags.append(fragment)

        return data_frags, parity_frags
    
    def calculate_packet_loss(self, received_fragments_count, transmission_time, fragments_sent):
        """Calculate the number of lost fragments."""
        lost_fragments = fragments_sent - received_fragments_count
        # print(f"Fragments sent: {fragments_sent}, Fragments received: {received_fragments_count}, Fragments lost: {lost_fragments}")
        # self.fragments_sent = 0

        if lost_fragments > 0:
            new_lambda = self.calculator.calculate_lambda(lost_fragments, transmission_time)
            print(f'Lost fragments: {lost_fragments}, transmission time: {transmission_time}')
            print(f"Sender: New calculated lambda: {new_lambda} at time {self.env.now}")
            self.calculator.lam = new_lambda
            self.lambdas.append(new_lambda)
            self.lambda_file.write(f"{new_lambda}\n")
            min_time, best_m, _ = self.calculator.find_min_time_configuration()
            print(f"Sender: New m parameters: {best_m}")
            self.env.process(self.update_m_parameters(best_m))
        yield self.env.timeout(0)

    def retransmit_chunks(self, missing_chunks):
        """Retransmit all fragments of missing chunks."""
        self.fragments_sent = 0
        for tier, chunks in missing_chunks.items():
            for chunk_id in chunks:
                # print(f"Retransmitting tier {tier} chunk {chunk_id}")
                data_frags, parity_frags = self.generate_chunk_fragments(tier, chunk_id, self.tier_frags_num[tier])
                batch_counter = 0
                for frag in data_frags + parity_frags:
                    yield self.env.timeout(1.0 / self.rate)
                    frag["time"] = self.env.now
                    self.link.put(frag)
                    self.fragments_sent += 1
                    self.total_fragments_sent += 1

                batch_counter += 1
                if batch_counter == CHUNK_BATCH_SIZE or chunk_id == chunks[-1]:
                    yield self.env.timeout(1.0 / self.rate)
                    control_msg = {"tier": tier, "chunk": chunk_id, "type": "control", "fragments_sent": self.fragments_sent}
                    self.link.put(control_msg)
                    batch_counter = 0
                    self.fragments_sent = 0
        
        last_frag = {"tier": -1, "chunk": 0, "fragment": 0, "type": "last_fragment"}
        self.link.put(last_frag)

    def update_m_parameters(self, new_m):
        """Update the m parameters."""
        for tier, m_value in new_m.items():
            self.tier_m[tier] = m_value
            # print(f"Updated m parameter for tier {tier} to {m_value}")
        yield self.env.timeout(0)

    def get_transmission_progress(self):
        return self.total_fragments_sent / self.total_fragments

class Receiver:

    def __init__(self, env, link, sender):
        self.env = env
        self.link = link
        self.all_tier_frags_received = {}
        self.all_tier_per_chunk_data_frags_num = {}
        self.sender = sender
        self.tier_start_times = {}
        self.tier_end_times = {}
        self.fragment_count = 0
        self.lost_chunk_per_tier = {}
        self.end_time = None
        self.first_frag_time = None
        self.last_frag_time = None
        # self.all_frags_received = False

    def receive(self):
        """A process which consumes packets."""
        while True:
            pkt = yield self.link.get()
            print(f'Received {pkt} at {self.env.now}')
            if pkt["type"] == "last_fragment":
                self.check_all_fragments_received()
                self.fragment_count = 0
            elif pkt["type"] == "control":
                self.last_frag_time = self.env.now
                
                if self.first_frag_time is not None:
                    self.send_received_fragments_count(self.fragment_count, self.last_frag_time - self.first_frag_time, pkt["fragments_sent"])
                    self.first_frag_time = None
                self.fragment_count = 0
            else:
                tier = pkt["tier"]
                chunk = pkt["chunk"]

                if self.first_frag_time is None:
                    self.first_frag_time = self.env.now

                if tier not in self.tier_start_times:
                    self.tier_start_times[tier] = self.env.now
                self.tier_end_times[tier] = self.env.now

                if tier not in self.all_tier_frags_received:
                    self.all_tier_frags_received[tier] = {}
                if chunk not in self.all_tier_frags_received[tier]:
                    self.all_tier_frags_received[tier][chunk] = 0

                self.all_tier_frags_received[tier][chunk] += 1
                
                self.fragment_count += 1

                self.update_data_frags_count(tier, chunk, pkt, pkt["type"])
               
    def update_data_frags_count(self, tier, chunk, fragment, frag_type):
        """Update the count of data fragments per chunk."""
        if tier not in self.all_tier_per_chunk_data_frags_num:
            self.all_tier_per_chunk_data_frags_num[tier] = {}
        if chunk not in self.all_tier_per_chunk_data_frags_num[tier]:
            self.all_tier_per_chunk_data_frags_num[tier][chunk] = 0

        if frag_type == "data":
            self.all_tier_per_chunk_data_frags_num[tier][chunk] = fragment["k"]
        elif frag_type == "parity" and chunk not in self.all_tier_per_chunk_data_frags_num[tier]:
            self.all_tier_per_chunk_data_frags_num[tier][chunk] = 32 - fragment["m"]

    def get_result(self):
        return self.all_tier_frags_received

    def check_all_fragments_received(self):
        print("Checking received fragments")
        self.check_paused = True
        missing_chunks = {}
        for tier, chunks in self.all_tier_frags_received.items():
            for chunk, count in chunks.items():
                # print(count, self.all_tier_per_chunk_data_frags_num[tier][chunk])
                if count < self.all_tier_per_chunk_data_frags_num[tier][chunk]:
                    if tier not in missing_chunks:
                        missing_chunks[tier] = []
                    missing_chunks[tier].append(chunk)
                    
                    self.all_tier_frags_received[tier][chunk] = 0

                    if tier in self.lost_chunk_per_tier:
                        self.lost_chunk_per_tier[tier] += 1
                    else:
                        self.lost_chunk_per_tier[tier] = 1

        if missing_chunks:
            print("Retransmitting missing chunks")
            self.env.process(self.sender.retransmit_chunks(missing_chunks))
        else:
            self.end_time = self.env.now
            print("All fragments received successfully!")
            if hasattr(self, 'packet_loss_gen'):
                self.packet_loss_gen.stop_loss_generation()
            # receiver.print_tier_receiving_times()
            # receiver.print_lost_chunks_per_tier()
        # else:
        #     self.all_frags_received = True

    def send_received_fragments_count(self, received_fragments_count, transmission_time, fragments_sent):
        """Send the count of received fragments to the sender."""
        self.env.process(self.sender.calculate_packet_loss(received_fragments_count, transmission_time, fragments_sent))

    def print_tier_receiving_times(self):
        total_overall = self.end_time - self.sender.start_time
        print(f"Total time from the beginning to the end: {total_overall}")

    def print_lost_chunks_per_tier(self):
        for tier in self.lost_chunk_per_tier:
            print(f"Tier: {tier}, amount of retransmitted chunks: {self.lost_chunk_per_tier[tier]}")

class PacketLossGen:
    def __init__(self, env, link, sender):
        self.env = env
        self.link = link
        self.sender = sender
        self.lambdas = [19, 383, 957]
        self.current_lambda = random.choice(self.lambdas)
        self.lambda_changes = [(self.env.now, self.current_lambda)]
        self.transmission_active = True 

        # Gaussian parameters (mean and standard deviation)
        self.mus_sigmas = {19: 2, 383: 40, 957: 100}
        self.current_lambda_gaus = 19

    def generate_lambda_from_gaussian(self):
        mu = random.choice(list(self.mus_sigmas.keys())) 
        sigma = self.mus_sigmas[mu] 
        lambda_value = max(1, random.gauss(mu, sigma))  # Ensure lambda is positive
        print(f'mu: {mu}, sigma: {sigma}, lambda: {lambda_value}')
        return lambda_value

    # def random_expovariate_loss_gen_gaus(self):
    #     while True:
    #         duration = random.uniform(5, 20) 
    #         end_time = self.env.now + duration
    #         print('')
    #         print(f"PacketLossGen: new lambda generated: {self.current_lambda_gaus} at time {self.env.now} for duration {duration}")
    #         while self.env.now < end_time:
    #             interval = random.expovariate(self.current_lambda_gaus) 
    #             yield self.env.timeout(interval)
    #             self.link.loss.put(f'A packet loss occurred at {self.env.now}')

    #         self.current_lambda_gaus = self.generate_lambda_from_gaussian()
    def random_expovariate_loss_gen_gaus(self):
        while self.transmission_active:  # Check the flag
            duration = random.uniform(5, 20) 
            end_time = self.env.now + duration
            print('')
            print(f"PacketLossGen: new lambda generated: {self.current_lambda_gaus} at time {self.env.now} for duration {duration}")
            while self.env.now < end_time and self.transmission_active:  # Check the flag
                interval = random.expovariate(self.current_lambda_gaus) 
                yield self.env.timeout(interval)
                if self.transmission_active:  # Check before putting
                    self.link.loss.put(f'A packet loss occurred at {self.env.now}')

            if not self.transmission_active:
                break
                
            self.current_lambda_gaus = self.generate_lambda_from_gaussian()

    def random_expovariate_loss_gen(self):
        while True:
            # Randomly choose a duration for using the current lambda
            duration = random.uniform(5, 20)
            end_time = self.env.now + duration

            while self.env.now < end_time:
                interval = random.expovariate(self.current_lambda)
                yield self.env.timeout(interval)
                self.link.loss.put(f'A packet loss occurred at {self.env.now}')

            self.current_lambda = random.choice(self.lambdas)
            self.lambda_changes.append((self.env.now, self.current_lambda))

    def expovariate_loss_gen(self, lambd):
        while True:
            yield self.env.timeout(random.expovariate(lambd))
            self.link.loss.put(f'A packet loss occurred at {self.env.now}')

    def random_loss_gen(self, min_time, max_time):
        while True:
            interval = random.uniform(min_time, max_time)
            yield self.env.timeout(interval)
            self.link.loss.put(f'A packet loss occurred at {self.env.now}')

    def stop_loss_generation(self):
        print(f"Stopping packet loss generation at {self.env.now}")
        self.transmission_active = False

def print_statistics(receiver):
    """Print statistics of the received fragments and calculate the recovery error."""
    lost_chunks_per_tier = {}
    result = receiver.get_result()
    all_tier_frags_received = receiver.all_tier_frags_received
    all_tier_per_chunk_data_frags_num = receiver.all_tier_per_chunk_data_frags_num

    for tier in all_tier_frags_received:
        for chunk in all_tier_frags_received[tier]:
            received_fragments = all_tier_frags_received[tier][chunk]
            required_fragments = all_tier_per_chunk_data_frags_num[tier][chunk]
            if received_fragments < required_fragments:
                print(f'Tier {tier} chunk {chunk} cannot be recovered since {required_fragments} fragments are needed while only {received_fragments} fragments were received.')
                if tier not in lost_chunks_per_tier:
                    lost_chunks_per_tier[tier] = []
                lost_chunks_per_tier[tier].append(chunk)

    total_error = get_recovery_error(lost_chunks_per_tier, sender.number_of_chunks)
    print(f"Total error: {total_error}")

def get_recovery_error(lost_chunks, number_of_chunks):
    """Calculate the recovery error based on lost chunks."""
    sum_error = 0
    error_per_tier = {}
    for tier, chunks in lost_chunks.items():
        error_per_tier[tier] = len(chunks) / number_of_chunks[tier]
        sum_error += len(chunks)
    for tier, error in error_per_tier.items():
        print(f"Tier: {tier}, error: {error}")
    return sum_error / sum(number_of_chunks)

def collect_and_output_statistics(receiver, sender, packet_loss_gen=None):
    """Collect and output comprehensive statistics after the end of transmission."""
    print("\n" + "="*80)
    print(" "*30 + "SIMULATION RESULTS SUMMARY")
    print("="*80)
    
    print("\n--- Simulation Parameters ---")
    print(f"Transmission Rate: {sender.rate} fragments/second")
    print(f"Fragment Size: {sender.calculator.frag_size} bytes")
    print(f"Number of Tiers: {len(sender.tier_frags_num)}")
    
    print("\n--- Time Statistics ---")
    end_time = receiver.end_time
    print(f"Total Transmission Time: {end_time}")
    
    for tier_id, start_time in receiver.tier_start_times.items():
        tier_end = receiver.tier_end_times.get(tier_id, "N/A")
        tier_time = tier_end - start_time if tier_end != "N/A" else "N/A"
        original_size_mb = sender.calculator.tier_sizes[tier_id] / (1024*1024)
        print(f"Tier {tier_id}: Start={start_time}, End={tier_end}")
        print(f"  - Size: {original_size_mb:.2f} MB")
        print(f"  - Fragments: {sender.tier_frags_num[tier_id]}")
        
        lost_chunks = receiver.lost_chunk_per_tier.get(tier_id, 0)
        if lost_chunks > 0:
            print(f"  - Retransmitted Chunks: {lost_chunks}")
    
    total_fragments_sent = sender.total_fragments_sent
    print(f"Total Fragments Sent: {total_fragments_sent}")
    
    print("\n--- Recovery Statistics ---")
    lost_chunks_per_tier = {}
    result = receiver.get_result()
    for tier in receiver.all_tier_frags_received:
        for chunk in receiver.all_tier_frags_received[tier]:
            received_fragments = receiver.all_tier_frags_received[tier][chunk]
            required_fragments = receiver.all_tier_per_chunk_data_frags_num[tier][chunk]
            if received_fragments < required_fragments:
                if tier not in lost_chunks_per_tier:
                    lost_chunks_per_tier[tier] = []
                lost_chunks_per_tier[tier].append(chunk)
                
    if lost_chunks_per_tier:
        print("Unrecovered Chunks:")
        for tier, chunks in lost_chunks_per_tier.items():
            print(f"  - Tier {tier}: {len(chunks)} chunks - {chunks}")
        total_error = get_recovery_error(lost_chunks_per_tier, sender.number_of_chunks)
        print(f"Total Recovery Error: {total_error:.4f}")
    else:
        print("All chunks successfully recovered")
    
    if end_time and start_time:
        total_bytes_sent = sum(sender.calculator.tier_sizes)
        print(f"Total Data Sent: {total_bytes_sent / (1024*1024):.2f} MB")
    

# rates = [1704.26, 6360.96, 10268.40, 15148.30, 21298.5, 24442.8, 25170.9, 26320.3, 27111.7, 27998.3, 28713.3]
# lambdas = [0.00001, 0.4518, 0.760058, 16.2197, 47.1776, 155.208, 563.973, 2777.1, 2539.07, 3181.82, 3528.7]
rates = [19144]
lambdas = [19]

now = datetime.datetime.now()

for i in range(len(rates)):
    rate = rates[i]
    lambd = lambdas[i]

    env = simpy.Environment()
    n = 32
    frag_size = 4096
    tier_sizes_orig = [5474475, 22402608, 45505266, 150891984] # 5.2 MB, 21.4 MB, 43.4 MB, 146.3 MB
    k = 8
    tier_sizes = [int(size * k) for size in tier_sizes_orig]

    tier_m = [0,0,0,0]

    t_trans = 0.01
    t_retrans = 0.01
    
    number_of_chunks = []
    min_times = []
    calculator = TransmissionTimeCalculator(tier_sizes, frag_size, t_trans, t_retrans, lambd, rate, n)
    min_time, tier_m, min_times = calculator.find_min_time_configuration()
    
    tier_frags_num = [i // frag_size + 1 for i in tier_sizes]

    link = Link(env, t_trans)
    sender = Sender(env, link, rate, tier_frags_num, tier_m, n, calculator)
    receiver = Receiver(env, link, sender)
    pkt_loss = PacketLossGen(env, link, sender)
    receiver.packet_loss_gen = pkt_loss

    env.process(sender.send())
    env.process(receiver.receive())
    # env.process(pkt_loss.expovariate_loss_gen(lambd))
    # env.process(pkt_loss.random_expovariate_loss_gen())
    env.process(pkt_loss.random_expovariate_loss_gen_gaus())

    env.run(until=SIM_DURATION)

    print(tier_frags_num)
    print_statistics(receiver)
    
    receiver.print_tier_receiving_times()
    receiver.print_lost_chunks_per_tier()

    collect_and_output_statistics(receiver, sender, pkt_loss)
    

# end = datetime.datetime.now()
# print("Time elapsed: ", end - now)