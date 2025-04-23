import math
from scipy.special import comb, gammaln
import itertools
from decimal import Decimal
from math import log, exp

class TransmissionTimeCalculator:
    
    @staticmethod
    def fault_tolerant_group_loss_prob_big_lambda(lambda_val, t_frag, rate_f, m):
        t_group = t_frag + (32 - 1) / rate_f

        mu = lambda_val*t_group/(t_group/(32/rate_f))
        
        cumulative_sum = sum(
            mu**i * math.exp(-mu) / math.factorial(i)
            for i in range(0, m + 1)
        )

        return 1-cumulative_sum

    @staticmethod
    def fault_tolerant_group_loss_prob_small_lambda(lambda_val, t_frag, rate_f, m):
        t_group = t_frag + (32 - 1) / rate_f
        L = int(rate_f * t_frag + 32 - 1)
        cumulative_sum = 0
        start = m + 1 if m > 0 else 1
        
        L=50
        for i in range(start, L + 1):
            # Poisson term using log for numerical stability  
            poisson_term = math.exp(i * math.log(lambda_val * t_group) - lambda_val * t_group)/math.factorial(i)
            
            # Binomial terms in the numerator
            numerator_sum = sum(
                comb(32, k) * comb(L - 32, i - k)
                for k in range(m + 1, min(i, 32) + 1)
            )

            denominator = comb(L, i)
            cumulative_sum += float(poisson_term * numerator_sum / denominator)
         
        return cumulative_sum
    
    @staticmethod
    def calculate_N_i(s_i, n, m_i, s_f):
        return s_i / ((n - m_i) * s_f)
    
    @staticmethod
    def calculate_lambda(lost_fragments, Ttrans):
        return lost_fragments / Ttrans

    @staticmethod
    def calculate_expected_epsilon(p_list, epsilon_list, N_list):
        """Calculate the expected value of epsilon using log arithmetic to avoid overflow."""
        expected_epsilon = 0
        product_term = exp(float(N_list[0] * log(1 - p_list[0])))
        expected_epsilon += (1-product_term)*1.0
        #print(p_list, epsilon_list)
        for i in range(1, len(p_list)-1):
            p = p_list[i]
            N = N_list[i]
            eps = epsilon_list[i]
            
            # Calculate (1-p)^N using log arithmetic
            if p != 1:  # Handle special case
                log_term = N * log(1 - p)
                power_term = exp(float(log_term)) 
            else:
                power_term = 0
            
            term = product_term * (1 - power_term) * eps
            expected_epsilon += term
            
            product_term *= power_term
            
        product_term *= exp(float(N_list[-1] * log(1 - p_list[-1])))
        expected_epsilon += product_term*epsilon_list[-1]

        return float(expected_epsilon)

    @staticmethod
    def calculate_T_total(T_f, n, N_list, r_f):
        
        N_sum = sum(N for N in N_list)
        return float(T_f + (n * N_sum - 1) / r_f)

    def find_optimal_m(self, lambda_val, t_frag, rate_f, n, tier_sizes, epsilon_list, s_f, T_threshold):
        """Find the optimal values of [m_1, ..., m_k] to minimize E[epsilon] while satisfying T_total constraint."""
        optimal_m = None
        min_expected_epsilon = float('inf')
        min_time = float('inf')

        t_ft_group = t_frag + (32 - 1) / rate_f
        frag_loss_per_ft_group = lambda_val*t_ft_group/(t_ft_group/(32/rate_f))
        
        candidate_m_values = []
        for m_values in itertools.product(range(17), repeat=len(tier_sizes)):
            try:
                N_list = [self.calculate_N_i(tier_sizes[i], n, m_values[i], s_f) for i in range(len(tier_sizes))]
                T_total = self.calculate_T_total(float(t_frag), n, N_list, float(rate_f))

                if T_total > float(T_threshold):
                    continue
                else:
                    candidate_m_values.append(m_values)


            except (OverflowError, ValueError) as e:
                print(f"Skipping m={m_values} due to numerical error: {str(e)}")
                continue

        for m_values in candidate_m_values:
        # for i in range(17):
            try:
                # m_values = [i] * len(tier_sizes)
                
                if frag_loss_per_ft_group > 1:
                    p_list = [self.fault_tolerant_group_loss_prob_big_lambda(float(lambda_val), float(t_frag), float(rate_f), m) for m in m_values]
                else:
                    p_list = [self.fault_tolerant_group_loss_prob_small_lambda(float(lambda_val), float(t_frag), float(rate_f), m) for m in m_values]

                N_list = [self.calculate_N_i(tier_sizes[i], n, m_values[i], s_f) for i in range(len(tier_sizes))]
                
                expected_epsilon = self.calculate_expected_epsilon(p_list, epsilon_list, N_list)

                #print("m_values", m_values, "expected_epsilon", expected_epsilon)
                if (expected_epsilon < min_expected_epsilon):
                    min_expected_epsilon = expected_epsilon
                    optimal_m = m_values
                elif expected_epsilon == min_expected_epsilon:
                    optimal_m = max(optimal_m, m_values)

            except (OverflowError, ValueError) as e:
                print(f"Skipping m={m_values} due to numerical error: {str(e)}")
                continue

        if optimal_m is None:
            raise Exception("No valid solution found within constraints")

        return optimal_m, min_expected_epsilon
    
   
# # Example usage:
# calculator = TransmissionTimeCalculator()
# lambda_val = 500
# t_frag = 0.01
# rate_f = 19144.6
# n = 32
# tier_sizes = [5474475, 22402608, 45505266, 150891984]
# tier_sizes = [x * 128 for x in tier_sizes]
# epsilon_list = [0.004, 0.0005, 0.00006, 0.0000001]
# # epsilon_list = [1.0, 1.0, 1.0, 1.0]
# s_f = 4096
# T_threshold = 388.8

# optimal_m, min_expected_epsilon = calculator.find_optimal_m(lambda_val, t_frag, rate_f, n, tier_sizes, epsilon_list, s_f, T_threshold)
# print("Lambda:", lambda_val)
# print(f"Optimal m values: {optimal_m}")
# print(f"Minimum expected epsilon: {min_expected_epsilon}")

