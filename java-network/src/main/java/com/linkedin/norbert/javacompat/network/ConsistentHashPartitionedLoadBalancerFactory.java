package com.linkedin.norbert.javacompat.network;

import com.linkedin.norbert.cluster.InvalidClusterException;

import java.util.HashSet;
import java.util.Set;

public class ConsistentHashPartitionedLoadBalancerFactory<PartitionedId> implements PartitionedLoadBalancerFactory<PartitionedId>
{
  private final int _bucketCount;
  private final HashFunction<String> _hashFunction;
  private final PartitionedLoadBalancerFactory<PartitionedId> _fallThrough;

  public ConsistentHashPartitionedLoadBalancerFactory(int bucketCount)
  {
    this(bucketCount, new HashFunction.MD5HashFunction(), null);
  }

  public ConsistentHashPartitionedLoadBalancerFactory(int bucketCount,
                                                      PartitionedLoadBalancerFactory<PartitionedId> fallThrough)
  {
    this(bucketCount, new HashFunction.MD5HashFunction(), fallThrough);
  }

  public ConsistentHashPartitionedLoadBalancerFactory(int bucketCount, HashFunction<String> hashFunction,
                                                      PartitionedLoadBalancerFactory<PartitionedId> fallThrough)
  {
    _bucketCount = bucketCount;
    _hashFunction = hashFunction;
    _fallThrough = fallThrough;
  }

  @Override
  public PartitionedLoadBalancer<PartitionedId> newLoadBalancer(Set<Endpoint> endpoints)
      throws InvalidClusterException
  {
//    PartitionedLoadBalancer<PartitionedId> inner = _fallThrough == null ? null : _fallThrough.newLoadBalancer(endpoints);
    return ConsistentHashPartitionedLoadBalancer.build(_bucketCount, _hashFunction, endpoints, null);
  }

  @Override
  public Integer getNumPartitions(Set<Endpoint> endpoints)
  {
    Set<Integer> set = new HashSet<Integer>();
    for (Endpoint endpoint : endpoints)
    {
      set.addAll(endpoint.getNode().getPartitionIds());
    }

    return set.size();
  }
}
