package io.mark.java_examples.Executables;

import java.io.Serializable;

public class AddPersonResponse  implements Serializable {
    private static final long serialVersionUID = 1234L;
    public String fullName;
    public int age;
    public boolean shouldExerciseMore;
}