package dlock.dlock;

public class DLockException extends Exception {
	private static final long serialVersionUID = -6387657109158610804L;
	
	public DLockException(String msg) {
		super(msg);
	}
	
	public DLockException(String msg, Throwable throwable) {
		super(msg, throwable);
	}
}
