#pragma once

#include <concepts>
#include <ostream>
#include <utility>



template<typename Tag, std::unsigned_integral T>
class UInt16StrongType {
    static_assert(sizeof(T) == 2);
    T value;
    public:
    explicit constexpr UInt16StrongType(T v) : value(v) {}

    // Makes sure not narrowing
    template<std::unsigned_integral U>
        requires (!std::same_as<U, T>)
    explicit constexpr UInt16StrongType(U v) : value{v} { 
        static_assert(std::in_range<T>(v) && "Value out of range for UInt16");

    }
    
    explicit UInt16StrongType() = default;

    // Arithmetic with self
    UInt16StrongType operator+(UInt16StrongType other) const { return value + other.value; }
    UInt16StrongType operator-(UInt16StrongType other) const { return value - other.value; }
    UInt16StrongType operator*(UInt16StrongType other) const { return value * other.value; }
    UInt16StrongType operator/(UInt16StrongType other) const { return value / other.value; }

    // Arithmetic with T - both sides
    friend constexpr UInt16StrongType operator+(const UInt16StrongType& lhs, T rhs) { return UInt16StrongType(lhs.value + rhs); }
    friend constexpr UInt16StrongType operator+(T lhs, const UInt16StrongType& rhs) { return UInt16StrongType(lhs + rhs.value); }

    friend constexpr UInt16StrongType operator-(const UInt16StrongType& lhs, T rhs) { return UInt16StrongType(lhs.value - rhs); }
    friend constexpr UInt16StrongType operator-(T lhs, const UInt16StrongType& rhs) { return UInt16StrongType(lhs - rhs.value); }

    friend constexpr UInt16StrongType operator*(const UInt16StrongType& lhs, T rhs) { return UInt16StrongType(lhs.value * rhs); }
    friend constexpr UInt16StrongType operator*(T lhs, const UInt16StrongType& rhs) { return UInt16StrongType(lhs * rhs.value); }

    friend constexpr UInt16StrongType operator/(const UInt16StrongType& lhs, T rhs) { return UInt16StrongType(lhs.value / rhs); }
    friend constexpr UInt16StrongType operator/(T lhs, const UInt16StrongType& rhs) { return UInt16StrongType(lhs / rhs.value); }

    // Comparison with self
    bool operator==(UInt16StrongType other) const { return value == other.value; }
    bool operator!=(UInt16StrongType other) const { return value != other.value; }
    bool operator< (UInt16StrongType other) const { return value <  other.value; }
    bool operator<=(UInt16StrongType other) const { return value <= other.value; }
    bool operator> (UInt16StrongType other) const { return value >  other.value; }
    bool operator>=(UInt16StrongType other) const { return value >= other.value; }

    // Comparison with T
    friend bool operator==(const UInt16StrongType& lhs, T rhs) { return lhs.value == rhs; }
    friend bool operator==(T lhs, const UInt16StrongType& rhs) { return lhs == rhs.value; }

    friend bool operator!=(const UInt16StrongType& lhs, T rhs) { return lhs.value != rhs; }
    friend bool operator!=(T lhs, const UInt16StrongType& rhs) { return lhs != rhs.value; }

    friend bool operator< (const UInt16StrongType& lhs, T rhs) { return lhs.value <  rhs; }
    friend bool operator< (T lhs, const UInt16StrongType& rhs) { return lhs <  rhs.value; }

    friend bool operator<=(const UInt16StrongType& lhs, T rhs) { return lhs.value <= rhs; }
    friend bool operator<=(T lhs, const UInt16StrongType& rhs) { return lhs <= rhs.value; }

    friend bool operator> (const UInt16StrongType& lhs, T rhs) { return lhs.value >  rhs; }
    friend bool operator> (T lhs, const UInt16StrongType& rhs) { return lhs >  rhs.value; }

    friend bool operator>=(const UInt16StrongType& lhs, T rhs) { return lhs.value >= rhs; }
    friend bool operator>=(T lhs, const UInt16StrongType& rhs) { return lhs >= rhs.value; }
    

    // Comparison with others
    template<std::unsigned_integral other_t>
    bool operator==(other_t other) const { return value == other; }

    // Can add to char ptr
    char* operator+(char* other) const { return value + other; }
    friend constexpr char* operator+(char* ptr, UInt16StrongType offset) { return ptr + offset.value; }
    friend constexpr char* operator+(UInt16StrongType offset, char* ptr) { return ptr + offset.value; }

    // Cout
    friend std::ostream& operator<<(std::ostream& os, const UInt16StrongType& obj) {
        return os << obj.value;
    }



    // Copy assignment from T
    constexpr UInt16StrongType& operator=(T other) noexcept {
        value = other;
        return *this;
    }
    // Delete all others
    template<typename U>
    requires (!std::same_as<U, T>)
        UInt16StrongType& operator=(U) = delete;



    // Explict conversion to T
    explicit constexpr operator T() const { return value; }

    // Explicit conversion to signed types that can contain the value
    template<std::signed_integral S>
        requires (sizeof(S) > sizeof(T))
    explicit constexpr operator S() const { 
        return static_cast<S>(value); 
    }

};

namespace lib_internal {
    template<typename Tag, std::integral T>
    class UInt16StrongType_Friend {
        using friend_t = UInt16StrongType<Tag, T>;
        friend friend_t;
        friend_t get(friend_t a) { return a.value; }
    };
}

template<typename Tag, std::integral T>
char* operator+(UInt16StrongType<Tag, T> a, char* other) { return lib_internal::UInt16StrongType_Friend{a}.get() + other; }

namespace lib_internal{
    struct OffsetTag {};
}
using offset_t = UInt16StrongType<lib_internal::OffsetTag, unsigned short>;