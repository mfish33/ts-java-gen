import { generateJavaFile, JavaInt, JavaString } from "./javaGen"

const testKey = {
    name: JavaString,
    id: JavaInt
}


console.log(generateJavaFile("TestKey", testKey))