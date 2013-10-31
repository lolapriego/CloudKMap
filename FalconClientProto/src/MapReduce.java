public interface MapReduce{
  public void map(String key, String value);
  public void reduce(String key, Iterator values);
  public void emitIntermediate(String key, String value);
}
