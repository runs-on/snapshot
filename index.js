const childProcess = require('child_process')
const os = require('os')
const process = require('process')

const ARGS = ''.split(',')
const WINDOWS = 'win32'
const LINUX = 'linux'
const AMD64 = 'x64'
const ARM64 = 'arm64'

function chooseBinary() {
    const platform = os.platform()
    const arch = os.arch()

    if (platform === LINUX && arch === AMD64) {
        return `main-linux-amd64`
    }
    if (platform === LINUX && arch === ARM64) {
        return `main-linux-arm64`
    }
    if (platform === WINDOWS && arch === AMD64) {
        return `main-windows-amd64`
    }

    console.error(`Unsupported platform (${platform}) and architecture (${arch})`)
    process.exit(1)
}

function main() {
    const binary = chooseBinary()
    const mainScript = `${__dirname}/${binary}`
    if (os.platform() === WINDOWS) {
        const args = ARGS.length > 0 ? `-ArgumentList "${ARGS.join(' ')}"` : ''
        childProcess.execFileSync('powershell', [
            '-Command',
            `Start-Process -FilePath "${mainScript}" ${args} -Verb RunAs -WindowStyle Hidden -Wait`
        ], { stdio: 'inherit' })
    } else {
        childProcess.execFileSync('sudo', ['-n', '-E', mainScript, ...ARGS], { stdio: 'inherit' })
    }
    process.exit(0)
}

if (require.main === module) {
    main()
}
