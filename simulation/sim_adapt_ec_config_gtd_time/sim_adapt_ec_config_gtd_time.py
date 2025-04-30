import simpy
import random
from formulaModule_sc2 import TransmissionTimeCalculator
import math

SIM_DURATION = 1000
CHUNK_BATCH_SIZE = 3200 # Number of chunks after which the sender sends a control message
TIME_WINDOW = 3.2

class Link:
    """This class represents the data transfer through a network link."""

    def __init__(self, env, delay):
        self.env = env
        self.delay = delay
        self.packets = simpy.Store(env)
        self.loss = simpy.Store(env, capacity=1)

    def transfer(self, value):
        yield self.env.timeout(self.delay)
        if len(self.loss.items) and value["type"] != "last_fragment" and value["type"] != "control":
            loss = yield self.loss.get()
            # print(f'{loss}, {value} got dropped')
        else:
            yield self.packets.put(value)

    def put(self, value):
        self.env.process(self.transfer(value))

    def get(self):
        return self.packets.get()

class Sender:

    def __init__(self, env, link, rate, tier_frags_num, tier_m, n, calculator, T_threshold):
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
        self.total_fragments_sent = 0
        self.total_fragments = sum(self.tier_frags_num)
        self.current_tier = 0
        self.T_threshold = T_threshold

    def send(self):
        """A process which generates and sends fragments by chunk."""
        if self.start_time is None:
            self.start_time = self.env.now

        for t in range(len(self.tier_frags_num)):
            print(f"Sending tier {t} fragments")
            remaining_frags_num = int(self.tier_frags_num[t])
            print("remaining_frags_num: ", remaining_frags_num)
            #last_chunk_id = frags_num // (self.n - self.tier_m[t])
            #total_chunks = last_chunk_id + 1
            batch_counter = 0 
            #print("Total chunks: ", total_chunks)

            ftg_id = 0
            while remaining_frags_num > 0:
                data_frags, parity_frags = self.generate_ftg_fragments(t, ftg_id, remaining_frags_num)
                #print(data_frags, parity_frags)
                self.fragments_sent += len(data_frags) + len(parity_frags)
                for frag in data_frags + parity_frags:
                    yield self.env.timeout(1.0 / self.rate)
                    frag["time"] = self.env.now
                    
                    self.total_fragments_sent += 1
                    frag["fragments_sent"] = self.total_fragments_sent
                    self.link.put(frag)

                batch_counter += 1

                decrement_value = (len(data_frags)) * 4096
                print(remaining_frags_num, tier_sizes[t], decrement_value)
                
                if tier_sizes[t] - decrement_value < 0:
                    print("Tier size is negative, setting to 0")
                    tier_sizes[t] = 0
                else:
                    tier_sizes[t] -= decrement_value

                if batch_counter == CHUNK_BATCH_SIZE:
                    control_msg = {"tier": t, "chunk": ftg_id, "type": "control", "fragments_sent": self.fragments_sent}
                    self.link.put(control_msg)
                    self.fragments_sent = 0
                    batch_counter = 0

                remaining_frags_num -= self.n - self.tier_m[t]   
                ftg_id += 1    

            self.number_of_chunks.append(ftg_id)
            self.current_tier += 1
        print("New tier sizes: ", tier_sizes)
        last_frag = {"tier": -1, "chunk": 0, "fragment": 0, "type": "last_fragment"}
        self.link.put(last_frag)
    
    def generate_ftg_fragments(self, tier, ftg_id, remaining_frags_num):
        """Generate data and parity fragments for a given chunk."""
        data_frags = []
        parity_frags = []

        k = self.n - self.tier_m[tier]
        m = self.tier_m[tier]
        if remaining_frags_num < self.n - self.tier_m[tier]: 
            k = remaining_frags_num
            m = self.n - k

        for i in range(0, k):
            fragment = {"tier": tier, "chunk": ftg_id, "fragment": i, "type": "data", "k": k}
            data_frags.append(fragment)

        for j in range(k, self.n):
            fragment = {"tier": tier, "chunk": ftg_id, "fragment": j, "type": "parity", "m": m}
            parity_frags.append(fragment)

        return data_frags, parity_frags

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

        if lost_fragments > 0 and sum(tier_sizes) > 0:
            new_lambda = self.calculator.calculate_lambda(lost_fragments, transmission_time)
            print(f'Lost fragments: {lost_fragments}, transmission time: {transmission_time}')
            print(f"Sender: New calculated lambda: {new_lambda} at time {self.env.now}")
            self.calculator.lam = new_lambda
            
            new_time = self.T_threshold - self.env.now
            # new_time = self.T_threshold
            print("Current tier_sizes: ", tier_sizes)
            print("New T_threshold: ", new_time, "time now: ", self.env.now)

            print(tier_sizes, epsilon_list)
            tmp1, tmp2 = zip(*[(x, y) for x, y in zip(tier_sizes, epsilon_list) if x != 0])
            new_tier_sizes, new_epsilon_list = list(tmp1), list(tmp2)
            print(new_tier_sizes, new_epsilon_list)

            best_m, min_expected_epsilon = calculator.find_optimal_m(new_lambda, t_frag=t_trans, rate_f=rate, n=n, tier_sizes=new_tier_sizes, epsilon_list=new_epsilon_list, s_f=frag_size, T_threshold=new_time)
            print(f"Sender: New m parameters: {best_m}")
            # self.env.process(self.update_m_parameters(self.get_optimal_m_for_lambda(new_lambda)))
            self.env.process(self.update_m_parameters(best_m))
        yield self.env.timeout(0)

    def retransmit_chunks(self, missing_chunks):
        """Retransmit all fragments of missing chunks using new erasure coding parameters."""
        self.fragments_sent = 0
        for tier, chunks in missing_chunks.items():
            for chunk_id in chunks:
                # print(f"Retransmitting tier {tier} chunk {chunk_id}")
                data_frags, parity_frags = self.generate_chunk_fragments(tier, chunk_id, self.tier_frags_num[tier])
                batch_counter = 0
                for frag in data_frags + parity_frags:
                    yield self.env.timeout(1.0 / self.rate)
                    frag["time"] = self.env.now
                    frag["fragments_sent"] = self.total_fragments_sent
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
                    self.current_tier += 1
        
        last_frag = {"tier": -1, "chunk": 0, "fragment": 0, "type": "last_fragment"}
        self.link.put(last_frag)

    def update_m_parameters(self, new_m):
        print("Current m configurations: ", self.tier_m)
        """Update the m parameters"""
        self.tier_m[-len(new_m):] = new_m
        print(f"Updated m parameter to {self.tier_m}")
        yield self.env.timeout(0)

    def get_transmission_progress(self):
        return self.total_fragments_sent / self.total_fragments

class Receiver:

    def __init__(self, env, link, sender, time_window):
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
        self.time_window = time_window
        self.all_lost_ftg = {}

    def receive(self):
        """A process which consumes packets."""
        pre_pkt_tier = 0
        pre_pkt_chunk = 0
        while True:
            pkt = yield self.link.get()
            print(f'Received {pkt} at {self.env.now}')
            if pkt["type"] == "last_fragment":
                #self.check_all_fragments_received()
                print(get_recovery_error(self.all_lost_ftg))
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
                # need to check if this is a new chunk (FTG)
                if tier != pre_pkt_tier or chunk != pre_pkt_chunk:
                    # this is a new chunk, need to check if previous chunk can be recovered
                    if self.all_tier_frags_received[pre_pkt_tier][pre_pkt_chunk] < self.all_tier_per_chunk_data_frags_num[pre_pkt_tier][pre_pkt_chunk]:
                        if pre_pkt_tier not in self.all_lost_ftg:
                            self.all_lost_ftg[pre_pkt_tier] = []
                        self.all_lost_ftg[pre_pkt_tier].append(pre_pkt_chunk)

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

                pre_pkt_tier = tier
                pre_pkt_chunk = chunk    

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
        new_tier_sizes = []
        self.check_paused = True
        missing_chunks = {}
        for tier, chunks in self.all_tier_frags_received.items():
            new_tier_sizes.append(0)
            print("Tier: ", tier, self.env.now)
            for chunk, count in chunks.items():
                # print(count, self.all_tier_per_chunk_data_frags_num[tier][chunk])
                if count < self.all_tier_per_chunk_data_frags_num[tier][chunk]:
                    if tier not in missing_chunks:
                        missing_chunks[tier] = []
                    missing_chunks[tier].append(chunk)
                    
                    #self.all_tier_frags_received[tier][chunk] = 0

                    if tier in self.lost_chunk_per_tier:
                        self.lost_chunk_per_tier[tier] += 1
                    else:
                        self.lost_chunk_per_tier[tier] = 1
                    # print(new_tier_sizes, tier)
                    # new_tier_sizes[tier] += 4096 * 32

        # tier_sizes = new_tier_sizes
        # print("Check fragments: New tier sizes: ", tier_sizes)

        # if missing_chunks:
        #     print("Retransmitting missing chunks")
        #     #add delay for retransmission
        #     self.env.process(self.sender.retransmit_chunks(missing_chunks))
        # else:
        #     print("All fragments received")
        #     self.end_time = self.env.now
        #     receiver.print_tier_receiving_times()
        #     receiver.print_lost_chunks_per_tier()
        #     print(get_recovery_error(missing_chunks))
        self.end_time = self.env.now
        print("End time:", self.end_time)
        print(get_recovery_error(missing_chunks))

    def send_received_fragments_count(self, received_fragments_count, transmission_time, fragments_sent):
        """Send the count of received fragments to the sender."""
        self.env.process(self.sender.calculate_packet_loss(received_fragments_count, transmission_time, fragments_sent))

    def print_tier_receiving_times(self):
        total = 0
        for tier in self.tier_start_times:
            start_time = self.tier_start_times[tier]
            end_time = self.tier_end_times.get(tier, start_time)
            total += end_time - start_time
            print(f"Tier {tier} receiving time: {end_time - start_time}")
        print(f"Total receiving time, which includes retransmission time for other tiers: {total}")
        if self.end_time is None:
            self.end_time = self.env.now
        total_overall = self.end_time - self.sender.start_time
        print(f"Total time from the beginning to the end: {total_overall}")

        return total

    def print_lost_chunks_per_tier(self):
        for tier in self.lost_chunk_per_tier:
            print(f"Tier: {tier}, amount of retransmitted chunks: {self.lost_chunk_per_tier[tier]}")

class PacketLossGen:
    def __init__(self, env, link, sender):
        self.env = env
        self.link = link
        self.sender = sender
        self.current_tier = None
        self.tier_progress = [0] * len(sender.tier_frags_num)
        # self.batch_size = 5 * CHUNK_BATCH_SIZE * 32  # Fragments per batch
        self.lambdas = [19, 383, 957]  # List of possible lambdas for packet loss
        self.current_lambda = random.choice(self.lambdas)
        self.lambda_changes = [(self.env.now, self.current_lambda)]

        # Gaussian parameters (mean and standard deviation)
        self.mus_sigmas = {19: 2, 383: 40, 957: 100}
        self.current_lambda_gaus = 19


    def generate_lambda_from_gaussian(self):
        mu = random.choice(list(self.mus_sigmas.keys())) 
        sigma = self.mus_sigmas[mu] 
        lambda_value = max(1, random.gauss(mu, sigma))  # Ensure lambda is positive
        print(f'mu: {mu}, sigma: {sigma}, lambda: {lambda_value}')
        return lambda_value

    def random_expovariate_loss_gen_gaus(self):
        while True:
            duration = random.uniform(5, 20) 
            end_time = self.env.now + duration
            print('')
            print(f"PacketLossGen: new lambda generated: {self.current_lambda_gaus} at time {self.env.now} for duration {duration}")
            while self.env.now < end_time:
                interval = random.expovariate(self.current_lambda_gaus) 
                yield self.env.timeout(interval)
                yield self.link.loss.put(f'A packet loss occurred at {self.env.now}')

            self.current_lambda_gaus = self.generate_lambda_from_gaussian()

    def random_expovariate_loss_gen(self):
        while True:
            # Randomly choose a duration (e.g., between 50 to 150 units of simulation time) for using the current lambda
            duration = random.uniform(5, 20)
            end_time = self.env.now + duration

            while self.env.now < end_time:
                interval = random.expovariate(self.current_lambda)
                yield self.env.timeout(interval)
                yield self.link.loss.put(f'A packet loss occurred at {self.env.now}')

            # Switch to a new lambda after the duration
            self.current_lambda = random.choice(self.lambdas)
            self.lambda_changes.append((self.env.now, self.current_lambda))

    def expovariate_loss_gen(self, lambd):
        while True:
            yield self.env.timeout(random.expovariate(lambd))
            yield self.link.loss.put(f'A packet loss occurred at {self.env.now}')


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
    
def get_recovery_error(lost_chunks):
    for tier, chunks in lost_chunks.items():
        #key = tuple(tier_m) if isinstance(tier_m, list) else tier_m
        key = "adaptive fault-tolerance configuration"
        if tier == 0:
            if key not in results_per_best_m:
                results_per_best_m[key] = {}
            if 'eps0' not in results_per_best_m[key]:
                results_per_best_m[key]['eps0'] = 0
            results_per_best_m[key]['eps0'] += 1
            return 'Data cannot be recovered, all levels are lost'
        else:
            if key not in results_per_best_m:
                results_per_best_m[key] = {}
            if 'eps' + str(tier) not in results_per_best_m[key]:
                results_per_best_m[key]['eps' + str(tier)] = 0
            results_per_best_m[key]['eps' + str(tier)] += 1
            return f'Data can be recovered to level {tier}, with error eps{tier}'
    return 'All tiers are recovered'


# rates = [1704.26, 6360.96, 10268.40, 15148.30, 21298.5, 24442.8, 25170.9, 26320.3, 27111.7, 27998.3, 28713.3]
# lambdas = [0.00001, 0.4518, 0.760058, 16.2197, 47.1776, 155.208, 563.973, 2777.1, 2539.07, 3181.82, 3528.7]
rates = [19144.6]
lambdas = [19]
epsilon_list = [0.004, 0.0005, 0.00006, 0.0000001]
n = 32
frag_size = 4096
tier_sizes_orig = [5474475, 22402608, 45505266, 150891984] # 5.2 MB, 21.4 MB, 43.4 MB, 146.3 MB
k = 8
t_trans = 0.01
t_retrans = 0.01
T_threshold = 388.8
time_window = TIME_WINDOW
results_per_best_m = {}
rate = 19144.6


for i in range(5):
    print(f"Run number: {i}")
    # rate = rates[i]
    # lambd = lambdas[i]
    tier_sizes = [int(size * k) for size in tier_sizes_orig]
    tier_frags_num = [math.ceil(i/frag_size) for i in tier_sizes]

    tier_sizes = [size * frag_size for size in tier_frags_num]
    print(f"Tier sizes: {tier_sizes}, tier fragments: {tier_frags_num}")

    env = simpy.Environment()
    
    number_of_chunks = []
    
    calculator = TransmissionTimeCalculator()
    # tier_m, min_expected_epsilon = calculator.find_optimal_m(lambd, t_frag=t_trans, rate_f=rate, n=n, tier_sizes=tier_sizes, epsilon_list=epsilon_list, s_f=frag_size, T_threshold=T_threshold)
    tier_m = [16, 0, 0, 0]
    

    link = Link(env, t_trans)
    sender = Sender(env, link, rate, tier_frags_num, tier_m, n, calculator, T_threshold)
    receiver = Receiver(env, link, sender, time_window)
    pkt_loss = PacketLossGen(env, link, sender)

    env.process(sender.send())
    env.process(receiver.receive())
    # env.process(pkt_loss.expovariate_loss_gen(lambd))
    # env.process(pkt_loss.random_expovariate_loss_gen())
    # env.process(pkt_loss.gaussian_loss_gen())
    env.process(pkt_loss.random_expovariate_loss_gen_gaus())

    env.run(until=SIM_DURATION)

    print(tier_frags_num)
    print_statistics(receiver)
    # print("Adaptive simulation results: rate: ", rate, " lambda: ", lambd)
    # receiver.print_tier_receiving_times()
    # receiver.print_lost_chunks_per_tier()

    link.packets = simpy.Store(env)
    link.loss = simpy.Store(env, capacity=1)


print("\nFinal EPS Error Counts per Tier for each best_m configuration:")
for best_m_config, value in results_per_best_m.items():
    print(f"\n--- Results for m = {best_m_config} ---")
    print(f'Eps values: {value}')
