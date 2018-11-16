package concurrentHashTables;

public interface TableType {
	public abstract void put(int value);
	public abstract void remove(int value);
	public abstract boolean get(int value);
}
