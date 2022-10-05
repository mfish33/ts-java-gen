type JavaType = {
    javaRepr: string
    readWriteType:string
    compare?:(a: string, b: string) => string
}

export const JavaString: JavaType = {
    javaRepr: "String",
    readWriteType: "Bytes"
}

export const JavaInt: JavaType = {
    javaRepr: "int",
    readWriteType: "Int",
    compare: (a, b) => `Integer.compare(${a}, ${b})`
}

export function generateJavaFile<T extends Record<string, JavaType>>(className: string, obj: T, sortBy?:(keyof T)[], partitionBy?:(keyof T)[]): string {
    return `
    import org.apache.hadoop.io.*;
    import java.io.*;
    
    public class ${className} implements Writable {
        ${Object.entries(obj).map(([varName, ty]) => `public ${ty.javaRepr} ${varName};`).join("\n    ")}
    
    
        public ${className}(${Object.entries(obj).map(([varName, ty]) => `${ty.javaRepr} ${varName}`).join(", ")}) {
            ${Object.entries(obj).map(([varName, ty]) => `this.${varName} = ${varName};`).join("\n        ")}
        }
    
        @Override
        public void readFields(DataInput in) throws IOException {
            ${Object.entries(obj).map(([varName, ty]) => `${varName}=in.read${ty.readWriteType}();`).join("\n        ")}
        }
    
        @Override
        public void write(DataOutput out) throws IOException {
            ${Object.entries(obj).map(([varName, ty]) => `out.write${ty.readWriteType}(${varName});`).join("\n        ")}
        }
    
        @Override
        public int compareTo(${className} other) {
            ${(() => {
                const sortByObj = Object.fromEntries((sortBy ?? Object.keys(obj)).map(key => [key, obj[key]])) as Partial<T>
                const checks = Object.entries(sortByObj).map(([varName, ty]) => generateCompareStatement(varName, ty)).join("\n")
                return checks + "\n        return 0;"
            })()}
        }
    
        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37).
            ${(partitionBy ?? Object.keys(obj)).map((varName) => `append(${String(varName)}).`).join("\n        ")}
            toHashCode();
        }
    }
    `
}
    
function generateCompareStatement(varName:string, ty: JavaType) {
return`
        int ${varName}Compare = ${ty.compare ? ty.compare(varName, `other.${varName}`) : `${varName}.compareTo(other.${varName})`};
        if(${varName}Compare != 0) {
            return ${varName}Compare;
        }
`
}
